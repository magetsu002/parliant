use clap::Parser;
use gtk::prelude::*;
use gtk::{Application, ApplicationWindow, Button, Label, Orientation, ToggleButton};
use gtk_layer_shell::{Edge, KeyboardMode, Layer, LayerShell};
use sidecar_ipc::{
    apply_daemon_event, default_socket_path, DaemonEvent, IpcClient, OverlayAction,
    OverlaySnapshot, TranscriptionUiState,
};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "sidecar-overlay", about = "Private local SIDECAR meeting overlay")]
struct Cli {
    /// Override the daemon Unix-domain socket path.
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[derive(Debug)]
enum UiUpdate {
    Connected(bool),
    Event(DaemonEvent),
}

fn main() {
    let cli = Cli::parse();
    let socket = match cli.socket.map(Ok).unwrap_or_else(default_socket_path) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("sidecar-overlay: {error}");
            return;
        }
    };

    let application = Application::builder()
        .application_id("dev.magetsu.sidecar.overlay")
        .build();
    application.connect_activate(move |app| build_ui(app, socket.clone()));
    application.run();
}

fn build_ui(application: &Application, socket: PathBuf) {
    let window = ApplicationWindow::builder()
        .application(application)
        .title("SIDECAR")
        .default_width(440)
        .default_height(260)
        .build();
    window.init_layer_shell();
    window.set_namespace("sidecar-overlay");
    window.set_layer(Layer::Overlay);
    window.set_anchor(Edge::Top, true);
    window.set_anchor(Edge::Right, true);
    window.set_layer_shell_margin(Edge::Top, 24);
    window.set_layer_shell_margin(Edge::Right, 24);
    window.set_keyboard_mode(KeyboardMode::OnDemand);

    let root = gtk::Box::new(Orientation::Vertical, 10);
    root.set_border_width(14);
    let status = Label::new(Some("Connecting to SIDECAR…"));
    status.set_xalign(0.0);
    let question = Label::new(None);
    question.set_xalign(0.0);
    question.set_line_wrap(true);
    question.set_selectable(true);
    let answer = Label::new(None);
    answer.set_xalign(0.0);
    answer.set_line_wrap(true);
    answer.set_selectable(true);
    let degraded = Label::new(None);
    degraded.set_xalign(0.0);
    degraded.set_line_wrap(true);

    let buttons = gtk::Box::new(Orientation::Horizontal, 8);
    let dismiss = Button::with_label("Dismiss");
    let copy = Button::with_label("Copy");
    let pin = ToggleButton::with_label("Pin");
    let stop = Button::with_label("Stop Listening");
    buttons.pack_start(&dismiss, false, false, 0);
    buttons.pack_start(&copy, false, false, 0);
    buttons.pack_start(&pin, false, false, 0);
    buttons.pack_end(&stop, false, false, 0);

    root.pack_start(&status, false, false, 0);
    root.pack_start(&question, false, false, 0);
    root.pack_start(&answer, true, true, 0);
    root.pack_start(&degraded, false, false, 0);
    root.pack_end(&buttons, false, false, 0);
    window.add(&root);

    let (action_tx, action_rx) = mpsc::sync_channel::<OverlayAction>(16);
    let (update_tx, update_rx) = mpsc::sync_channel::<UiUpdate>(32);
    spawn_ipc_worker(socket, action_rx, update_tx);

    let dismiss_tx = action_tx.clone();
    dismiss.connect_clicked(move |_| {
        let _ = dismiss_tx.try_send(OverlayAction::Dismiss);
    });
    let stop_tx = action_tx.clone();
    stop.connect_clicked(move |_| {
        let _ = stop_tx.try_send(OverlayAction::StopListening);
    });

    let answer_for_copy = answer.clone();
    let window_for_copy = window.clone();
    copy.connect_clicked(move |_| {
        let text = answer_for_copy.text();
        if !text.is_empty() {
            window_for_copy
                .clipboard(&gdk::SELECTION_CLIPBOARD)
                .set_text(text.as_str());
        }
    });
    let window_for_pin = window.clone();
    pin.connect_toggled(move |button| {
        window_for_pin.set_keep_above(button.is_active());
    });

    let status_label = status.clone();
    let question_label = question.clone();
    let answer_label = answer.clone();
    let degraded_label = degraded.clone();
    let snapshot = std::rc::Rc::new(std::cell::RefCell::new(OverlaySnapshot::default()));
    let connected = std::rc::Rc::new(std::cell::Cell::new(false));
    let snapshot_for_tick = std::rc::Rc::clone(&snapshot);
    let connected_for_tick = std::rc::Rc::clone(&connected);
    gtk::glib::timeout_add_local(Duration::from_millis(50), move || {
        while let Ok(update) = update_rx.try_recv() {
            match update {
                UiUpdate::Connected(value) => connected_for_tick.set(value),
                UiUpdate::Event(event) => {
                    apply_daemon_event(&mut snapshot_for_tick.borrow_mut(), &event);
                }
            }
        }
        let current = snapshot_for_tick.borrow();
        let connection = if connected_for_tick.get() {
            "connected"
        } else {
            "disconnected · retrying"
        };
        let listening = if current.listening { "listening" } else { "stopped" };
        let transcription = match current.transcription {
            TranscriptionUiState::Idle => "idle",
            TranscriptionUiState::Connecting => "transcription connecting",
            TranscriptionUiState::Streaming => "transcribing",
            TranscriptionUiState::Degraded => "transcription degraded",
        };
        status_label.set_text(&format!("{connection} · {listening} · {transcription}"));
        question_label.set_text(current.question.as_deref().unwrap_or(""));
        answer_label.set_text(&current.answer);
        let diagnostic = current
            .error
            .as_deref()
            .or(current.degraded.as_deref())
            .unwrap_or("");
        degraded_label.set_text(diagnostic);
        gtk::glib::ControlFlow::Continue
    });

    window.show_all();
}

fn spawn_ipc_worker(
    socket: PathBuf,
    action_rx: mpsc::Receiver<OverlayAction>,
    update_tx: mpsc::SyncSender<UiUpdate>,
) {
    let _ = thread::Builder::new()
        .name("sidecar-overlay-ipc-client".to_string())
        .spawn(move || loop {
            match IpcClient::connect(&socket) {
                Ok(mut client) => {
                    let _ = update_tx.try_send(UiUpdate::Connected(true));
                    loop {
                        while let Ok(action) = action_rx.try_recv() {
                            if client.send_action(action).is_err() {
                                break;
                            }
                        }
                        match client.recv_event_timeout(Duration::from_millis(100)) {
                            Ok(Some(event)) => {
                                let _ = update_tx.try_send(UiUpdate::Event(event));
                            }
                            Ok(None) => {}
                            Err(_) => break,
                        }
                    }
                }
                Err(_) => {
                    let _ = update_tx.try_send(UiUpdate::Connected(false));
                }
            }
            thread::sleep(Duration::from_millis(500));
        });
}
