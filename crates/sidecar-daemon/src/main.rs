use clap::{Parser, Subcommand};
use sidecar_audio::{
    bounded_frame_channel, install_termination_signals, run_pipewire_capture, CancellationToken,
    CaptureEvent, CaptureMode, CaptureTarget, StopReason,
};
use std::process::ExitCode;
use std::sync::mpsc;
use std::thread;

#[derive(Debug, Parser)]
#[command(name = "sidecar", about = "SIDECAR local meeting intelligence daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Capture one explicitly selected PipeWire source or sink monitor.
    Capture {
        /// PipeWire node.name or object.serial. SIDECAR never chooses a fallback target.
        #[arg(long)]
        target: String,

        /// Capture the selected sink's monitor ports instead of a source node.
        #[arg(long)]
        sink_monitor: bool,

        /// Maximum number of audio frames queued in memory before newest-frame drops begin.
        #[arg(long, default_value_t = 64)]
        buffer_frames: usize,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Capture {
            target,
            sink_monitor,
            buffer_frames,
        } => run_capture(target, sink_monitor, buffer_frames),
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
            eprintln!("sidecar: {error}");
            return ExitCode::from(2);
        }
    };

    let (frame_tx, frame_rx) = match bounded_frame_channel(buffer_frames) {
        Ok(channel) => channel,
        Err(error) => {
            eprintln!("sidecar: {error}");
            return ExitCode::from(2);
        }
    };
    let stats_probe = frame_tx.clone();
    let cancellation = CancellationToken::new();
    if let Err(error) = install_termination_signals(&cancellation) {
        eprintln!("sidecar: {error}");
        return ExitCode::FAILURE;
    }

    let consumer = thread::spawn(move || {
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        while let Ok(frame) = frame_rx.recv() {
            frames = frames.saturating_add(1);
            bytes = bytes.saturating_add(frame.data.len() as u64);
        }
        (frames, bytes)
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
    let (consumed_frames, consumed_bytes) = consumer.join().unwrap_or((0, 0));
    let _ = event_printer.join();

    eprintln!(
        "sidecar: capture summary accepted={} dropped_full={} consumed_frames={} consumed_bytes={}",
        stats.accepted, stats.dropped_full, consumed_frames, consumed_bytes
    );

    match result {
        Ok(StopReason::Cancelled) => ExitCode::SUCCESS,
        Ok(StopReason::SourceLost) => ExitCode::from(3),
        Ok(StopReason::BackendError) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("sidecar: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_event(event: &CaptureEvent) {
    match event {
        CaptureEvent::Starting { target } => eprintln!(
            "sidecar: starting target={} mode={:?}",
            target.object(),
            target.mode()
        ),
        CaptureEvent::Connecting => eprintln!("sidecar: connecting"),
        CaptureEvent::FormatNegotiated(format) => eprintln!(
            "sidecar: negotiated {} Hz, {} channel(s), {:?}",
            format.sample_rate_hz, format.channels, format.sample_format
        ),
        CaptureEvent::Streaming => eprintln!("sidecar: streaming"),
        CaptureEvent::SourceLost => eprintln!("sidecar: selected source lost"),
        CaptureEvent::Error(message) => eprintln!("sidecar: capture error: {message}"),
        CaptureEvent::Stopped(reason) => eprintln!("sidecar: stopped: {reason:?}"),
    }
}
