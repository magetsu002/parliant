use clap::{Parser, Subcommand};
use parliant_audio::{
    bounded_frame_channel, install_termination_signals, run_pipewire_capture, CancellationToken,
    CaptureEvent, CaptureMode, CaptureTarget, StopReason,
};
use parliant_context::{MeetingState, MeetingStoreConfig};
use parliant_detect::{HeuristicSemanticClassifier, QuestionDetectorConfig};
use parliant_ipc::{
    default_socket_path, DaemonEvent, IpcServer, IpcServerHandle, OverlayAction,
    TranscriptionUiState,
};
use parliant_mcp::McpService;
use parliant_orchestrator::{
    AnswerError, AnswerEvent, AnswerProvider, AnswerRequest, AnswerSession, OpenAiResponsesConfig,
    OpenAiResponsesProvider,
};
use parliant_remote_mcp::{RemoteBridgeConfig, RemoteMcpBridge};
use parliant_runtime::{RuntimeEngine, RuntimeStats};
use parliant_transcribe::{
    OpenAiRealtimeConfig, OpenAiRealtimeProvider, TranscriptionError, TranscriptionProvider,
    TranscriptionSession,
};
use std::cell::Cell;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(
    name = "parliant",
    about = "PARLIANT local meeting intelligence daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Capture one explicitly selected PipeWire source or sink monitor.
    Capture {
        /// PipeWire node.name or object.serial. PARLIANT never chooses a fallback target.
        #[arg(long)]
        target: String,

        /// Capture the selected sink's monitor ports instead of a source node.
        #[arg(long)]
        sink_monitor: bool,

        /// Maximum number of audio frames queued in memory before newest-frame drops begin.
        #[arg(long, default_value_t = 64)]
        buffer_frames: usize,
    },

    /// Run the complete local V1 meeting pipeline.
    Meet {
        /// PipeWire node.name or object.serial. PARLIANT never chooses a fallback target.
        #[arg(long)]
        target: String,

        /// Capture the selected sink's monitor ports instead of a source node.
        #[arg(long)]
        sink_monitor: bool,

        /// Maximum number of captured audio frames queued in memory.
        #[arg(long, default_value_t = 64)]
        buffer_frames: usize,

        /// Optional OpenAI Responses API model used for local answer suggestions.
        #[arg(long)]
        answer_model: Option<String>,

        /// Environment variable containing the OpenAI API key.
        #[arg(long, default_value = "OPENAI_API_KEY")]
        api_key_env: String,

        /// Optional environment variable containing private response instructions.
        #[arg(long, default_value = "PARLIANT_USER_INSTRUCTIONS")]
        user_instructions_env: String,

        /// Override the private overlay Unix-domain socket path.
        #[arg(long)]
        overlay_socket: Option<PathBuf>,

        /// Run without the presentation overlay IPC server.
        #[arg(long)]
        no_overlay: bool,

        /// Explicitly enable the loopback-only remote MCP bridge.
        #[arg(long)]
        remote_mcp: bool,

        /// Loopback address for the opt-in remote MCP bridge. Port 0 selects a free local port.
        #[arg(long, default_value = "127.0.0.1:0")]
        remote_mcp_bind: SocketAddr,

        /// Environment variable containing the remote MCP bearer token.
        #[arg(long, default_value = "PARLIANT_REMOTE_MCP_TOKEN")]
        remote_mcp_token_env: String,

        /// Browser Origin allowed to call the remote MCP endpoint. May be repeated.
        #[arg(long)]
        remote_origin: Vec<String>,

        /// Answer timeout before the current local suggestion is cancelled.
        #[arg(long, default_value_t = 20)]
        answer_timeout_seconds: u64,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Capture {
            target,
            sink_monitor,
            buffer_frames,
        } => run_capture(target, sink_monitor, buffer_frames),
        Command::Meet {
            target,
            sink_monitor,
            buffer_frames,
            answer_model,
            api_key_env,
            user_instructions_env,
            overlay_socket,
            no_overlay,
            remote_mcp,
            remote_mcp_bind,
            remote_mcp_token_env,
            remote_origin,
            answer_timeout_seconds,
        } => match run_meeting(MeetingOptions {
            target,
            sink_monitor,
            buffer_frames,
            answer_model,
            api_key_env,
            user_instructions_env,
            overlay_socket,
            no_overlay,
            remote_mcp,
            remote_mcp_bind,
            remote_mcp_token_env,
            remote_origin,
            answer_timeout: Duration::from_secs(answer_timeout_seconds.max(1)),
        }) {
            Ok(code) => code,
            Err(error) => {
                eprintln!("parliant: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

#[derive(Debug)]
struct MeetingOptions {
    target: String,
    sink_monitor: bool,
    buffer_frames: usize,
    answer_model: Option<String>,
    api_key_env: String,
    user_instructions_env: String,
    overlay_socket: Option<PathBuf>,
    no_overlay: bool,
    remote_mcp: bool,
    remote_mcp_bind: SocketAddr,
    remote_mcp_token_env: String,
    remote_origin: Vec<String>,
    answer_timeout: Duration,
}

enum LocalAnswerProvider {
    Disabled,
    OpenAi(OpenAiResponsesProvider),
}

impl LocalAnswerProvider {
    fn from_model(api_key: &str, model: Option<String>) -> Result<Self, String> {
        match model {
            Some(model) => {
                let config = OpenAiResponsesConfig::new(api_key.to_string(), model)
                    .map_err(|error| error.to_string())?;
                Ok(Self::OpenAi(OpenAiResponsesProvider::new(config)))
            }
            None => Ok(Self::Disabled),
        }
    }

    fn is_enabled(&self) -> bool {
        matches!(self, Self::OpenAi(_))
    }
}

impl AnswerProvider for LocalAnswerProvider {
    fn start(&self, request: AnswerRequest) -> Result<Box<dyn AnswerSession>, AnswerError> {
        match self {
            Self::Disabled => Ok(Box::new(DisabledAnswerSession::default())),
            Self::OpenAi(provider) => provider.start(request),
        }
    }
}

#[derive(Default)]
struct DisabledAnswerSession {
    completed: Cell<bool>,
}

impl AnswerSession for DisabledAnswerSession {
    fn recv_timeout(&self, _timeout: Duration) -> Result<Option<AnswerEvent>, AnswerError> {
        if self.completed.replace(true) {
            Ok(None)
        } else {
            Ok(Some(AnswerEvent::Done))
        }
    }

    fn cancel(&self) {
        self.completed.set(true);
    }
}

fn run_meeting(options: MeetingOptions) -> Result<ExitCode, String> {
    let api_key = std::env::var(&options.api_key_env)
        .map_err(|_| format!("{} must be set", options.api_key_env))?;
    let user_instructions = std::env::var(&options.user_instructions_env)
        .ok()
        .filter(|value| !value.trim().is_empty());

    let target = CaptureTarget::new(
        options.target,
        if options.sink_monitor {
            CaptureMode::SinkMonitor
        } else {
            CaptureMode::Source
        },
    )
    .map_err(|error| error.to_string())?;
    let (frame_tx, frame_rx) =
        bounded_frame_channel(options.buffer_frames).map_err(|error| error.to_string())?;
    let capture_stats = frame_tx.clone();

    let cancellation = CancellationToken::new();
    install_termination_signals(&cancellation).map_err(|error| error.to_string())?;

    let meeting_state = Arc::new(RwLock::new(
        MeetingState::new(MeetingStoreConfig::default()).map_err(|error| error.to_string())?,
    ));
    meeting_state
        .write()
        .map_err(|_| "meeting state lock unavailable".to_string())?
        .start_session();

    let overlay = if options.no_overlay {
        None
    } else {
        let socket = match options.overlay_socket {
            Some(path) => path,
            None => default_socket_path().map_err(|error| error.to_string())?,
        };
        let server = IpcServer::bind(socket)
            .map_err(|error| error.to_string())?
            .spawn()
            .map_err(|error| error.to_string())?;
        Some(Arc::new(server))
    };
    publish(overlay.as_deref(), DaemonEvent::Listening { active: true });
    publish(
        overlay.as_deref(),
        DaemonEvent::Transcription {
            state: TranscriptionUiState::Connecting,
        },
    );

    let remote_bridge = if options.remote_mcp {
        let token = std::env::var(&options.remote_mcp_token_env).map_err(|_| {
            format!(
                "{} must be set when --remote-mcp is enabled",
                options.remote_mcp_token_env
            )
        })?;
        let mut config = RemoteBridgeConfig::enabled_loopback(options.remote_mcp_bind, token)
            .map_err(|error| error.to_string())?;
        for origin in options.remote_origin {
            config = config
                .allow_origin(origin)
                .map_err(|error| error.to_string())?;
        }
        let bridge = RemoteMcpBridge::start(config, McpService::new(Arc::clone(&meeting_state)))
            .map_err(|error| error.to_string())?;
        eprintln!(
            "parliant: remote MCP enabled on loopback {} (authenticated; transport tunnel required for cloud access)",
            bridge.local_addr()
        );
        Some(bridge)
    } else {
        None
    };

    let transcriber = OpenAiRealtimeProvider::new(
        OpenAiRealtimeConfig::new(api_key.clone()).map_err(|error| error.to_string())?,
    );
    let transcription_session = transcriber.connect().map_err(|error| error.to_string())?;
    let answer_provider = LocalAnswerProvider::from_model(&api_key, options.answer_model)?;
    if !answer_provider.is_enabled() {
        eprintln!("parliant: local answer generation disabled; meeting context remains available through MCP");
    }
    let engine = RuntimeEngine::new(
        Arc::clone(&meeting_state),
        HeuristicSemanticClassifier,
        QuestionDetectorConfig::default(),
        answer_provider,
        options.answer_timeout,
        user_instructions,
    );

    let pipeline_cancellation = cancellation.clone();
    let pipeline_overlay = overlay.clone();
    let pipeline = thread::Builder::new()
        .name("parliant-v1-pipeline".to_string())
        .spawn(move || {
            run_pipeline(
                frame_rx,
                transcription_session,
                engine,
                pipeline_overlay,
                pipeline_cancellation,
            )
        })
        .map_err(|error| format!("failed to start V1 pipeline: {error}"))?;

    let (capture_event_tx, capture_event_rx) = mpsc::channel();
    let event_overlay = overlay.clone();
    let capture_events = thread::Builder::new()
        .name("parliant-capture-events".to_string())
        .spawn(move || {
            while let Ok(event) = capture_event_rx.recv() {
                print_event(&event);
                match event {
                    CaptureEvent::Streaming => publish(
                        event_overlay.as_deref(),
                        DaemonEvent::Listening { active: true },
                    ),
                    CaptureEvent::SourceLost => publish(
                        event_overlay.as_deref(),
                        DaemonEvent::Error {
                            message: "selected audio source was lost".to_string(),
                        },
                    ),
                    CaptureEvent::Error(_) => publish(
                        event_overlay.as_deref(),
                        DaemonEvent::Error {
                            message: "audio capture backend reported an error".to_string(),
                        },
                    ),
                    CaptureEvent::Stopped(_) => publish(
                        event_overlay.as_deref(),
                        DaemonEvent::Listening { active: false },
                    ),
                    CaptureEvent::Starting { .. }
                    | CaptureEvent::Connecting
                    | CaptureEvent::FormatNegotiated(_) => {}
                }
            }
        })
        .map_err(|error| format!("failed to start capture event observer: {error}"))?;

    let capture_result =
        run_pipewire_capture(target, frame_tx, capture_event_tx, cancellation.clone());
    cancellation.cancel();

    let pipeline_stats = pipeline
        .join()
        .map_err(|_| "V1 pipeline thread panicked".to_string())?;
    let _ = capture_events.join();

    if let Some(bridge) = &remote_bridge {
        bridge.stop();
    }
    if let Some(server) = &overlay {
        publish(
            Some(server.as_ref()),
            DaemonEvent::Listening { active: false },
        );
        server.stop();
    }

    let capture_stats = capture_stats.stats();
    eprintln!(
        "parliant: V1 summary captured={} capture_dropped={} transcribe_backpressure={} finalized={} duplicates={} questions={} answer_deltas={} answer_failures={} transcription_failures={} first_answer_latency_ms={:?}",
        capture_stats.accepted,
        capture_stats.dropped_full,
        pipeline_stats.transcribe_backpressure,
        pipeline_stats.runtime.finalized_segments,
        pipeline_stats.runtime.duplicate_segments,
        pipeline_stats.runtime.detected_questions,
        pipeline_stats.runtime.answer_deltas,
        pipeline_stats.runtime.answer_failures,
        pipeline_stats.runtime.transcription_failures,
        pipeline_stats.runtime.first_answer_latency_ms,
    );

    match capture_result {
        Ok(StopReason::Cancelled) => Ok(ExitCode::SUCCESS),
        Ok(StopReason::SourceLost) => Ok(ExitCode::from(3)),
        Ok(StopReason::BackendError) => Ok(ExitCode::FAILURE),
        Err(error) => Err(error.to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct PipelineStats {
    runtime: RuntimeStats,
    transcribe_backpressure: u64,
}

fn run_pipeline(
    frame_rx: mpsc::Receiver<parliant_core::AudioFrame>,
    transcription: Box<dyn TranscriptionSession>,
    mut engine: RuntimeEngine<LocalAnswerProvider, HeuristicSemanticClassifier>,
    overlay: Option<Arc<IpcServerHandle>>,
    cancellation: CancellationToken,
) -> PipelineStats {
    let mut transcribe_backpressure = 0_u64;
    while !cancellation.is_cancelled() {
        match frame_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(frame) => match transcription.push_audio(frame) {
                Ok(()) => {}
                Err(TranscriptionError::AudioBackpressure) => {
                    transcribe_backpressure = transcribe_backpressure.saturating_add(1);
                }
                Err(_) => {
                    publish(
                        overlay.as_deref(),
                        DaemonEvent::Degraded {
                            message: "transcription session became unavailable".to_string(),
                        },
                    );
                    cancellation.cancel();
                    break;
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        for _ in 0..32 {
            match transcription.recv_event_timeout(Duration::ZERO) {
                Ok(Some(event)) => match engine.handle_transcription(event) {
                    Ok(events) => {
                        for event in events {
                            publish(overlay.as_deref(), event);
                        }
                    }
                    Err(_) => publish(
                        overlay.as_deref(),
                        DaemonEvent::Degraded {
                            message: "meeting reasoning path is temporarily degraded".to_string(),
                        },
                    ),
                },
                Ok(None) => break,
                Err(_) => {
                    publish(
                        overlay.as_deref(),
                        DaemonEvent::Degraded {
                            message: "transcription event stream became unavailable".to_string(),
                        },
                    );
                    cancellation.cancel();
                    break;
                }
            }
        }

        for _ in 0..16 {
            match engine.poll_answer(Duration::ZERO) {
                Ok(Some(event)) => publish(overlay.as_deref(), event),
                Ok(None) => break,
                Err(_) => {
                    publish(
                        overlay.as_deref(),
                        DaemonEvent::Degraded {
                            message: "answer provider is temporarily unavailable".to_string(),
                        },
                    );
                    break;
                }
            }
        }

        if let Some(server) = overlay.as_deref() {
            for _ in 0..8 {
                match server.recv_action_timeout(Duration::ZERO) {
                    Ok(Some(action)) => match action.action {
                        OverlayAction::Dismiss => {
                            if let Some(event) =
                                engine.handle_overlay_action(OverlayAction::Dismiss)
                            {
                                publish(Some(server), event);
                            }
                        }
                        OverlayAction::StopListening => {
                            cancellation.cancel();
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }

    transcription.cancel();
    engine.shutdown();
    PipelineStats {
        runtime: engine.stats(),
        transcribe_backpressure,
    }
}

fn publish(server: Option<&IpcServerHandle>, event: DaemonEvent) {
    if let Some(server) = server {
        let _ = server.publish(event);
    }
}

fn run_capture(target: String, sink_monitor: bool, buffer_frames: usize) -> ExitCode {
    let mode = if sink_monitor {
        CaptureMode::SinkMonitor
    } else {
        CaptureMode::Source
    };
    let target = match CaptureTarget::new(target, mode) {
        Ok(target) => target,
        Err(error) => {
            eprintln!("parliant: {error}");
            return ExitCode::from(2);
        }
    };

    let (frame_tx, frame_rx) = match bounded_frame_channel(buffer_frames) {
        Ok(channel) => channel,
        Err(error) => {
            eprintln!("parliant: {error}");
            return ExitCode::from(2);
        }
    };
    let stats_probe = frame_tx.clone();
    let cancellation = CancellationToken::new();
    if let Err(error) = install_termination_signals(&cancellation) {
        eprintln!("parliant: {error}");
        return ExitCode::FAILURE;
    }

    let consumer = thread::spawn(move || {
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut first_timestamp_ns = None;
        let mut last_timestamp_ns = None;
        let mut timestamps_monotonic = true;

        while let Ok(frame) = frame_rx.recv() {
            let timestamp_ns = frame.timestamp.nanos_since_start;
            if first_timestamp_ns.is_none() {
                first_timestamp_ns = Some(timestamp_ns);
            }
            if let Some(previous_timestamp_ns) = last_timestamp_ns {
                timestamps_monotonic &= timestamp_ns >= previous_timestamp_ns;
            }
            last_timestamp_ns = Some(timestamp_ns);
            frames = frames.saturating_add(1);
            bytes = bytes.saturating_add(frame.data.len() as u64);
        }

        (
            frames,
            bytes,
            first_timestamp_ns,
            last_timestamp_ns,
            timestamps_monotonic,
        )
    });

    let (event_tx, event_rx) = mpsc::channel();
    let event_printer = thread::spawn(move || {
        while let Ok(event) = event_rx.recv() {
            print_event(&event);
        }
    });

    let result = run_pipewire_capture(target, frame_tx, event_tx, cancellation);

    let stats = stats_probe.stats();
    drop(stats_probe);
    let (
        consumed_frames,
        consumed_bytes,
        first_timestamp_ns,
        last_timestamp_ns,
        timestamps_monotonic,
    ) = consumer.join().unwrap_or((0, 0, None, None, false));
    let _ = event_printer.join();
    let timestamps_advanced = matches!(
        (first_timestamp_ns, last_timestamp_ns),
        (Some(first), Some(last)) if last > first
    );

    eprintln!(
        "parliant: capture summary accepted={} dropped_full={} consumed_frames={} consumed_bytes={} first_timestamp_ns={first_timestamp_ns:?} last_timestamp_ns={last_timestamp_ns:?} timestamps_monotonic={timestamps_monotonic} timestamps_advanced={timestamps_advanced}",
        stats.accepted, stats.dropped_full, consumed_frames, consumed_bytes
    );

    match result {
        Ok(StopReason::Cancelled) => ExitCode::SUCCESS,
        Ok(StopReason::SourceLost) => ExitCode::from(3),
        Ok(StopReason::BackendError) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("parliant: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_event(event: &CaptureEvent) {
    match event {
        CaptureEvent::Starting { target } => eprintln!(
            "parliant: starting target={} mode={:?}",
            target.object(),
            target.mode()
        ),
        CaptureEvent::Connecting => eprintln!("parliant: connecting"),
        CaptureEvent::FormatNegotiated(format) => eprintln!(
            "parliant: negotiated {} Hz, {} channel(s), {:?}",
            format.sample_rate_hz, format.channels, format.sample_format
        ),
        CaptureEvent::Streaming => eprintln!("parliant: streaming"),
        CaptureEvent::SourceLost => eprintln!("parliant: selected source lost"),
        CaptureEvent::Error(_message) => eprintln!("parliant: capture backend error"),
        CaptureEvent::Stopped(reason) => eprintln!("parliant: stopped: {reason:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parliant_core::MonotonicTimestamp;
    use parliant_transcribe::{TranscriptSegment, TranscriptionEvent};

    fn parse_meet(args: &[&str]) -> Command {
        Cli::try_parse_from(args).unwrap().command
    }

    #[test]
    fn meet_accepts_omitted_answer_model_without_selecting_a_default() {
        let command = parse_meet(&[
            "parliant",
            "meet",
            "--target",
            "fixture-sink",
            "--sink-monitor",
            "--no-overlay",
        ]);
        let Command::Meet { answer_model, .. } = command else {
            panic!("expected meet command");
        };
        assert_eq!(answer_model, None);
    }

    #[test]
    fn supplying_answer_model_selects_existing_openai_suggestion_path() {
        let command = parse_meet(&[
            "parliant",
            "meet",
            "--target",
            "fixture-sink",
            "--answer-model",
            "fixture-answer-model",
        ]);
        let Command::Meet { answer_model, .. } = command else {
            panic!("expected meet command");
        };
        assert_eq!(answer_model.as_deref(), Some("fixture-answer-model"));

        let provider = LocalAnswerProvider::from_model("fixture-api-key", answer_model).unwrap();
        assert!(matches!(provider, LocalAnswerProvider::OpenAi(_)));
    }

    #[test]
    fn chatgpt_only_mode_keeps_transcript_and_mcp_state_without_responses_request() {
        let provider = LocalAnswerProvider::from_model("fixture-api-key", None).unwrap();
        assert!(matches!(provider, LocalAnswerProvider::Disabled));

        let mut state = MeetingState::new(MeetingStoreConfig::default()).unwrap();
        state.start_session();
        let shared = Arc::new(RwLock::new(state));
        let mut runtime = RuntimeEngine::new(
            Arc::clone(&shared),
            HeuristicSemanticClassifier,
            QuestionDetectorConfig {
                cooldown_segments: 0,
                ..QuestionDetectorConfig::default()
            },
            provider,
            Duration::from_secs(1),
            None,
        );

        let segment = TranscriptSegment::new(
            "chatgpt-only-1",
            "When will you deploy the release?",
            MonotonicTimestamp::from_nanos(10),
            MonotonicTimestamp::from_nanos(20),
            None,
        )
        .unwrap();
        let events = runtime
            .handle_transcription(TranscriptionEvent::Final(segment))
            .unwrap();

        assert_eq!(shared.read().unwrap().status().retained_segments, 1);
        assert!(matches!(events.as_slice(), [DaemonEvent::Question { .. }]));
        assert!(matches!(
            runtime.poll_answer(Duration::ZERO).unwrap(),
            Some(DaemonEvent::AnswerDone { .. })
        ));
        assert_eq!(runtime.stats().answer_deltas, 0);
        assert_eq!(runtime.stats().answer_failures, 0);

        let mcp = McpService::new(Arc::clone(&shared));
        let response = mcp
            .handle_json(
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"meeting_get_recent","arguments":{"limit":5}}}"#,
            )
            .unwrap()
            .unwrap();
        assert!(response.contains("When will you deploy the release?"));
    }
}
