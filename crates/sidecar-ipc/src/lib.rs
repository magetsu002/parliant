//! Versioned, bounded local IPC between the SIDECAR daemon and presentation-only overlay.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use thiserror::Error;

pub const IPC_VERSION: u16 = 1;
pub const MAX_WIRE_BYTES: usize = 64 * 1024;
pub const MAX_QUESTION_CHARS: usize = 2_048;
pub const MAX_ANSWER_CHARS: usize = 16_384;
pub const MAX_STATUS_CHARS: usize = 1_024;
pub const DEFAULT_CLIENT_QUEUE: usize = 32;
pub const DEFAULT_ACTION_QUEUE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptionUiState {
    #[default]
    Idle,
    Connecting,
    Streaming,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OverlaySnapshot {
    pub listening: bool,
    pub transcription: TranscriptionUiState,
    pub question_id: Option<u64>,
    pub question: Option<String>,
    pub answer: String,
    pub answer_complete: bool,
    pub degraded: Option<String>,
    pub error: Option<String>,
}

impl OverlaySnapshot {
    pub fn sanitize(mut self) -> Self {
        self.question = self
            .question
            .map(|text| truncate_chars(&text, MAX_QUESTION_CHARS));
        self.answer = truncate_chars(&self.answer, MAX_ANSWER_CHARS);
        self.degraded = self
            .degraded
            .map(|text| truncate_chars(&text, MAX_STATUS_CHARS));
        self.error = self
            .error
            .map(|text| truncate_chars(&text, MAX_STATUS_CHARS));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEvent {
    Snapshot { state: OverlaySnapshot },
    Listening { active: bool },
    Transcription { state: TranscriptionUiState },
    Question { id: u64, text: String },
    AnswerDelta { question_id: u64, delta: String },
    AnswerDone { question_id: u64 },
    Degraded { message: String },
    Error { message: String },
    Dismissed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OverlayAction {
    Dismiss,
    StopListening,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WireEnvelope<T> {
    version: u16,
    payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayActionEnvelope {
    pub client_id: u64,
    pub action: OverlayAction,
}

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("XDG_RUNTIME_DIR is not available")]
    RuntimeDirUnavailable,
    #[error("IPC queue capacity must be greater than zero")]
    InvalidQueueCapacity,
    #[error("refusing to replace a non-socket IPC path")]
    UnsafeSocketPath,
    #[error("IPC peer uid does not match the SIDECAR process uid")]
    PeerUidMismatch,
    #[error("IPC message exceeds the configured bound")]
    MessageTooLarge,
    #[error("IPC protocol version mismatch: expected {expected}, received {received}")]
    VersionMismatch { expected: u16, received: u16 },
    #[error("invalid IPC JSON: {0}")]
    InvalidJson(String),
    #[error("IPC connection closed")]
    ConnectionClosed,
    #[error("IPC action queue is full")]
    ActionBackpressure,
    #[error("IPC server state lock poisoned")]
    StateUnavailable,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn default_socket_path() -> Result<PathBuf, IpcError> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR").ok_or(IpcError::RuntimeDirUnavailable)?;
    Ok(PathBuf::from(runtime).join("sidecar").join("overlay.sock"))
}

#[derive(Debug)]
struct ClientSink {
    id: u64,
    tx: mpsc::SyncSender<DaemonEvent>,
}

pub struct IpcServer {
    listener: UnixListener,
    path: PathBuf,
    queue_capacity: usize,
    action_capacity: usize,
    snapshot: OverlaySnapshot,
}

impl IpcServer {
    pub fn bind(path: impl AsRef<Path>) -> Result<Self, IpcError> {
        Self::bind_with_capacities(path, DEFAULT_CLIENT_QUEUE, DEFAULT_ACTION_QUEUE)
    }

    pub fn bind_with_capacities(
        path: impl AsRef<Path>,
        queue_capacity: usize,
        action_capacity: usize,
    ) -> Result<Self, IpcError> {
        if queue_capacity == 0 || action_capacity == 0 {
            return Err(IpcError::InvalidQueueCapacity);
        }
        let path = path.as_ref().to_path_buf();
        prepare_socket_path(&path)?;
        let listener = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            path,
            queue_capacity,
            action_capacity,
            snapshot: OverlaySnapshot::default(),
        })
    }

    pub fn with_snapshot(mut self, snapshot: OverlaySnapshot) -> Self {
        self.snapshot = snapshot.sanitize();
        self
    }

    pub fn spawn(self) -> Result<IpcServerHandle, IpcError> {
        let clients = Arc::new(Mutex::new(Vec::<ClientSink>::new()));
        let snapshot = Arc::new(Mutex::new(self.snapshot));
        let shutdown = Arc::new(AtomicBool::new(false));
        let next_client_id = Arc::new(AtomicU64::new(1));
        let (action_tx, action_rx) = mpsc::sync_channel(self.action_capacity);

        let listener = self.listener;
        let queue_capacity = self.queue_capacity;
        let thread_clients = Arc::clone(&clients);
        let thread_snapshot = Arc::clone(&snapshot);
        let thread_shutdown = Arc::clone(&shutdown);
        let thread_next_client_id = Arc::clone(&next_client_id);
        let thread_action_tx = action_tx.clone();
        let accept_thread = thread::Builder::new()
            .name("sidecar-overlay-ipc".to_string())
            .spawn(move || {
                while !thread_shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let client_id = thread_next_client_id.fetch_add(1, Ordering::Relaxed);
                            let clients = Arc::clone(&thread_clients);
                            let snapshot = Arc::clone(&thread_snapshot);
                            let action_tx = thread_action_tx.clone();
                            let _ = thread::Builder::new()
                                .name(format!("sidecar-overlay-client-{client_id}"))
                                .spawn(move || {
                                    let _ = handle_client(
                                        client_id,
                                        stream,
                                        queue_capacity,
                                        clients,
                                        snapshot,
                                        action_tx,
                                    );
                                });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(20));
                        }
                        Err(_) => break,
                    }
                }
            })?;

        Ok(IpcServerHandle {
            path: self.path,
            clients,
            snapshot,
            action_rx: Mutex::new(action_rx),
            shutdown,
            accept_thread: Mutex::new(Some(accept_thread)),
        })
    }
}

pub struct IpcServerHandle {
    path: PathBuf,
    clients: Arc<Mutex<Vec<ClientSink>>>,
    snapshot: Arc<Mutex<OverlaySnapshot>>,
    action_rx: Mutex<mpsc::Receiver<OverlayActionEnvelope>>,
    shutdown: Arc<AtomicBool>,
    accept_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl IpcServerHandle {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn snapshot(&self) -> Result<OverlaySnapshot, IpcError> {
        self.snapshot
            .lock()
            .map(|state| state.clone())
            .map_err(|_| IpcError::StateUnavailable)
    }

    pub fn publish(&self, event: DaemonEvent) -> Result<usize, IpcError> {
        let event = sanitize_event(event);
        {
            let mut state = self
                .snapshot
                .lock()
                .map_err(|_| IpcError::StateUnavailable)?;
            apply_daemon_event(&mut state, &event);
        }
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| IpcError::StateUnavailable)?;
        clients.retain(|client| client.tx.try_send(event.clone()).is_ok());
        Ok(clients.len())
    }

    pub fn recv_action_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<OverlayActionEnvelope>, IpcError> {
        let receiver = self
            .action_rx
            .lock()
            .map_err(|_| IpcError::StateUnavailable)?;
        match receiver.recv_timeout(timeout) {
            Ok(action) => Ok(Some(action)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(IpcError::ConnectionClosed),
        }
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(mut join) = self.accept_thread.lock() {
            if let Some(handle) = join.take() {
                let _ = handle.join();
            }
        }
        let _ = fs::remove_file(&self.path);
    }
}

impl Drop for IpcServerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(join) = self.accept_thread.get_mut() {
            if let Some(handle) = join.take() {
                let _ = handle.join();
            }
        }
        let _ = fs::remove_file(&self.path);
    }
}

pub struct IpcClient {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl IpcClient {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, IpcError> {
        let writer = UnixStream::connect(path)?;
        verify_peer_uid(&writer)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Self { reader, writer })
    }

    pub fn send_action(&mut self, action: OverlayAction) -> Result<(), IpcError> {
        write_envelope(&mut self.writer, &action)
    }

    pub fn recv_event(&mut self) -> Result<DaemonEvent, IpcError> {
        read_envelope(&mut self.reader)
    }

    pub fn recv_event_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<DaemonEvent>, IpcError> {
        if !wait_readable(self.reader.get_ref(), timeout)? {
            return Ok(None);
        }
        self.recv_event().map(Some)
    }
}

pub fn apply_daemon_event(state: &mut OverlaySnapshot, event: &DaemonEvent) {
    match event {
        DaemonEvent::Snapshot { state: replacement } => *state = replacement.clone().sanitize(),
        DaemonEvent::Listening { active } => state.listening = *active,
        DaemonEvent::Transcription { state: next } => state.transcription = next.clone(),
        DaemonEvent::Question { id, text } => {
            state.question_id = Some(*id);
            state.question = Some(truncate_chars(text, MAX_QUESTION_CHARS));
            state.answer.clear();
            state.answer_complete = false;
            state.error = None;
        }
        DaemonEvent::AnswerDelta { question_id, delta } => {
            if state.question_id == Some(*question_id) {
                let remaining = MAX_ANSWER_CHARS.saturating_sub(state.answer.chars().count());
                state.answer.extend(delta.chars().take(remaining));
                state.answer_complete = false;
            }
        }
        DaemonEvent::AnswerDone { question_id } => {
            if state.question_id == Some(*question_id) {
                state.answer_complete = true;
            }
        }
        DaemonEvent::Degraded { message } => {
            state.degraded = Some(truncate_chars(message, MAX_STATUS_CHARS));
        }
        DaemonEvent::Error { message } => {
            state.error = Some(truncate_chars(message, MAX_STATUS_CHARS));
        }
        DaemonEvent::Dismissed => {
            state.question_id = None;
            state.question = None;
            state.answer.clear();
            state.answer_complete = false;
            state.error = None;
        }
    }
}

fn handle_client(
    client_id: u64,
    stream: UnixStream,
    queue_capacity: usize,
    clients: Arc<Mutex<Vec<ClientSink>>>,
    snapshot: Arc<Mutex<OverlaySnapshot>>,
    action_tx: mpsc::SyncSender<OverlayActionEnvelope>,
) -> Result<(), IpcError> {
    verify_peer_uid(&stream)?;
    let writer_stream = stream.try_clone()?;
    let (event_tx, event_rx) = mpsc::sync_channel(queue_capacity);
    let current = snapshot
        .lock()
        .map_err(|_| IpcError::StateUnavailable)?
        .clone();
    event_tx
        .try_send(DaemonEvent::Snapshot { state: current })
        .map_err(|_| IpcError::ConnectionClosed)?;
    clients
        .lock()
        .map_err(|_| IpcError::StateUnavailable)?
        .push(ClientSink {
            id: client_id,
            tx: event_tx,
        });

    let writer = thread::Builder::new()
        .name(format!("sidecar-overlay-writer-{client_id}"))
        .spawn(move || {
            let mut stream = writer_stream;
            while let Ok(event) = event_rx.recv() {
                if write_envelope(&mut stream, &event).is_err() {
                    break;
                }
            }
        })?;

    let mut reader = BufReader::new(stream);
    let result = loop {
        match read_envelope::<OverlayAction, _>(&mut reader) {
            Ok(action) => {
                let envelope = OverlayActionEnvelope { client_id, action };
                match action_tx.try_send(envelope) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full(_)) => {
                        break Err(IpcError::ActionBackpressure);
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        break Err(IpcError::ConnectionClosed);
                    }
                }
            }
            Err(IpcError::ConnectionClosed) => break Ok(()),
            Err(error) => break Err(error),
        }
    };
    if let Ok(mut clients) = clients.lock() {
        clients.retain(|client| client.id != client_id);
    }
    let _ = writer.join();
    result
}

fn wait_readable(stream: &UnixStream, timeout: Duration) -> Result<bool, IpcError> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let mut descriptor = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
        if result > 0 {
            return Ok((descriptor.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) != 0);
        }
        if result == 0 {
            return Ok(false);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(IpcError::Io(error));
    }
}

fn prepare_socket_path(path: &Path) -> Result<(), IpcError> {
    let parent = path.parent().ok_or(IpcError::UnsafeSocketPath)?;
    fs::create_dir_all(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(path)?,
        Ok(_) => return Err(IpcError::UnsafeSocketPath),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(IpcError::Io(error)),
    }
    Ok(())
}

fn verify_peer_uid(stream: &UnixStream) -> Result<(), IpcError> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(IpcError::Io(std::io::Error::last_os_error()));
    }
    let process_uid = unsafe { libc::geteuid() };
    if credentials.uid != process_uid {
        return Err(IpcError::PeerUidMismatch);
    }
    Ok(())
}

fn write_envelope<T: Serialize, W: Write>(writer: &mut W, payload: &T) -> Result<(), IpcError> {
    let encoded = serde_json::to_vec(&WireEnvelope {
        version: IPC_VERSION,
        payload,
    })
    .map_err(|error| IpcError::InvalidJson(error.to_string()))?;
    if encoded.len().saturating_add(1) > MAX_WIRE_BYTES {
        return Err(IpcError::MessageTooLarge);
    }
    writer.write_all(&encoded)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn read_envelope<T: DeserializeOwned, R: BufRead>(reader: &mut R) -> Result<T, IpcError> {
    let mut line = Vec::new();
    let bytes = reader
        .take((MAX_WIRE_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)?;
    if bytes == 0 {
        return Err(IpcError::ConnectionClosed);
    }
    if line.len() > MAX_WIRE_BYTES {
        return Err(IpcError::MessageTooLarge);
    }
    let envelope: WireEnvelope<T> =
        serde_json::from_slice(&line).map_err(|error| IpcError::InvalidJson(error.to_string()))?;
    if envelope.version != IPC_VERSION {
        return Err(IpcError::VersionMismatch {
            expected: IPC_VERSION,
            received: envelope.version,
        });
    }
    Ok(envelope.payload)
}

fn sanitize_event(event: DaemonEvent) -> DaemonEvent {
    match event {
        DaemonEvent::Snapshot { state } => DaemonEvent::Snapshot {
            state: state.sanitize(),
        },
        DaemonEvent::Question { id, text } => DaemonEvent::Question {
            id,
            text: truncate_chars(&text, MAX_QUESTION_CHARS),
        },
        DaemonEvent::AnswerDelta { question_id, delta } => DaemonEvent::AnswerDelta {
            question_id,
            delta: truncate_chars(&delta, MAX_ANSWER_CHARS),
        },
        DaemonEvent::Degraded { message } => DaemonEvent::Degraded {
            message: truncate_chars(&message, MAX_STATUS_CHARS),
        },
        DaemonEvent::Error { message } => DaemonEvent::Error {
            message: truncate_chars(&message, MAX_STATUS_CHARS),
        },
        other => other,
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn socket_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sidecar-ipc-{name}-{}-{unique}.sock",
            std::process::id()
        ))
    }

    #[test]
    fn socket_is_owner_only_and_actions_round_trip() {
        let path = socket_path("roundtrip");
        let server = IpcServer::bind(&path).unwrap().spawn().unwrap();
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let mut client = IpcClient::connect(&path).unwrap();
        let first = client.recv_event().unwrap();
        assert!(matches!(first, DaemonEvent::Snapshot { .. }));
        client.send_action(OverlayAction::StopListening).unwrap();
        let action = server
            .recv_action_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(action.action, OverlayAction::StopListening);
        server.stop();
    }

    #[test]
    fn reconnect_receives_latest_bounded_snapshot() {
        let path = socket_path("reconnect");
        let server = IpcServer::bind(&path).unwrap().spawn().unwrap();
        server
            .publish(DaemonEvent::Listening { active: true })
            .unwrap();
        server
            .publish(DaemonEvent::Question {
                id: 7,
                text: "q".repeat(MAX_QUESTION_CHARS + 50),
            })
            .unwrap();

        let mut first = IpcClient::connect(&path).unwrap();
        let DaemonEvent::Snapshot { state } = first.recv_event().unwrap() else {
            panic!("expected snapshot");
        };
        assert!(state.listening);
        assert_eq!(state.question_id, Some(7));
        assert_eq!(state.question.unwrap().chars().count(), MAX_QUESTION_CHARS);
        drop(first);

        server
            .publish(DaemonEvent::AnswerDelta {
                question_id: 7,
                delta: "answer".to_string(),
            })
            .unwrap();
        let mut second = IpcClient::connect(&path).unwrap();
        let DaemonEvent::Snapshot { state } = second.recv_event().unwrap() else {
            panic!("expected snapshot");
        };
        assert_eq!(state.answer, "answer");
        server.stop();
    }

    #[test]
    fn malformed_and_wrong_version_messages_are_rejected() {
        let path = socket_path("malformed");
        let server = IpcServer::bind(&path).unwrap().spawn().unwrap();

        let mut malformed = UnixStream::connect(&path).unwrap();
        malformed.write_all(b"not-json\n").unwrap();
        malformed.flush().unwrap();
        thread::sleep(Duration::from_millis(50));
        assert!(server
            .recv_action_timeout(Duration::from_millis(50))
            .unwrap()
            .is_none());

        let mut wrong = UnixStream::connect(&path).unwrap();
        wrong
            .write_all(b"{\"version\":999,\"payload\":{\"type\":\"dismiss\"}}\n")
            .unwrap();
        wrong.flush().unwrap();
        thread::sleep(Duration::from_millis(50));
        assert!(server
            .recv_action_timeout(Duration::from_millis(50))
            .unwrap()
            .is_none());
        server.stop();
    }

    #[test]
    fn answer_and_status_payloads_are_bounded() {
        let mut state = OverlaySnapshot {
            question_id: Some(3),
            question: Some("q".repeat(MAX_QUESTION_CHARS + 1)),
            answer: "a".repeat(MAX_ANSWER_CHARS + 1),
            degraded: Some("d".repeat(MAX_STATUS_CHARS + 1)),
            error: Some("e".repeat(MAX_STATUS_CHARS + 1)),
            ..OverlaySnapshot::default()
        }
        .sanitize();
        assert_eq!(
            state.question.as_ref().unwrap().chars().count(),
            MAX_QUESTION_CHARS
        );
        assert_eq!(state.answer.chars().count(), MAX_ANSWER_CHARS);
        assert_eq!(
            state.degraded.as_ref().unwrap().chars().count(),
            MAX_STATUS_CHARS
        );
        assert_eq!(
            state.error.as_ref().unwrap().chars().count(),
            MAX_STATUS_CHARS
        );

        apply_daemon_event(
            &mut state,
            &DaemonEvent::AnswerDelta {
                question_id: 3,
                delta: "extra".repeat(100),
            },
        );
        assert_eq!(state.answer.chars().count(), MAX_ANSWER_CHARS);
    }

    #[test]
    fn non_socket_path_is_never_clobbered() {
        let path = socket_path("safe-path");
        fs::write(&path, "do not replace").unwrap();
        assert!(matches!(
            IpcServer::bind(&path),
            Err(IpcError::UnsafeSocketPath)
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), "do not replace");
        fs::remove_file(path).unwrap();
    }
}
