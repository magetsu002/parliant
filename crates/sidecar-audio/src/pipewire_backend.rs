use crate::{
    BoundedFrameSender, CancellationToken, CaptureError, CaptureEvent, CaptureMode, CaptureTarget,
    StopReason,
};
use pipewire as pw;
use pw::properties::properties;
use pw::spa;
use spa::param::format::{MediaSubtype, MediaType};
use spa::param::format_utils;
use spa::pod::Pod;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use sidecar_core::{AudioFormat, AudioFrame, MonotonicTimestamp, SampleFormat};

struct UserData {
    negotiated: spa::param::audio::AudioInfoRaw,
    format: Option<AudioFormat>,
    frames: BoundedFrameSender,
    events: mpsc::Sender<CaptureEvent>,
    session_start: Instant,
    sequence: u64,
    saw_streaming: bool,
}

/// Runs one explicitly-targeted PipeWire capture session on the current thread.
///
/// The stream uses `DONT_RECONNECT` plus `node.dont-reconnect`/`node.dont-fallback`, so loss of
/// the chosen target is surfaced rather than silently moving capture to another device or app.
pub fn run_pipewire_capture(
    target: CaptureTarget,
    frames: BoundedFrameSender,
    events: mpsc::Sender<CaptureEvent>,
    cancellation: CancellationToken,
) -> Result<StopReason, CaptureError> {
    pw::init();

    let _ = events.send(CaptureEvent::Starting {
        target: target.clone(),
    });

    let mainloop = pw::main_loop::MainLoopRc::new(None)
        .map_err(|error| CaptureError::Backend(error.to_string()))?;
    let context = pw::context::ContextRc::new(&mainloop, None)
        .map_err(|error| CaptureError::Backend(error.to_string()))?;
    let core = context
        .connect_rc(None)
        .map_err(|error| CaptureError::Backend(error.to_string()))?;

    let props = match target.mode() {
        CaptureMode::Source => properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Communication",
            *pw::keys::TARGET_OBJECT => target.object(),
            *pw::keys::NODE_DONT_RECONNECT => "true",
            "node.dont-fallback" => "true",
        },
        CaptureMode::SinkMonitor => properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Communication",
            *pw::keys::TARGET_OBJECT => target.object(),
            *pw::keys::NODE_DONT_RECONNECT => "true",
            "node.dont-fallback" => "true",
            *pw::keys::STREAM_CAPTURE_SINK => "true",
        },
    };

    let stream = pw::stream::StreamBox::new(&core, "sidecar-capture", props)
        .map_err(|error| CaptureError::Backend(error.to_string()))?;

    let data = UserData {
        negotiated: spa::param::audio::AudioInfoRaw::new(),
        format: None,
        frames,
        events: events.clone(),
        session_start: Instant::now(),
        sequence: 0,
        saw_streaming: false,
    };

    let state_loop = mainloop.clone();
    let _listener = stream
        .add_local_listener_with_user_data(data)
        .state_changed(move |_stream, user_data, _old, new| match new {
            pw::stream::StreamState::Connecting => {
                let _ = user_data.events.send(CaptureEvent::Connecting);
            }
            pw::stream::StreamState::Streaming => {
                user_data.saw_streaming = true;
                let _ = user_data.events.send(CaptureEvent::Streaming);
            }
            pw::stream::StreamState::Error(message) => {
                let _ = user_data.events.send(CaptureEvent::Error(message));
                state_loop.quit();
            }
            pw::stream::StreamState::Unconnected if user_data.saw_streaming => {
                let _ = user_data.events.send(CaptureEvent::SourceLost);
                state_loop.quit();
            }
            pw::stream::StreamState::Unconnected | pw::stream::StreamState::Paused => {}
        })
        .param_changed(|_stream, user_data, id, param| {
            let Some(param) = param else {
                user_data.format = None;
                return;
            };
            if id != spa::param::ParamType::Format.as_raw() {
                return;
            }

            let Ok((media_type, media_subtype)) = format_utils::parse_format(param) else {
                return;
            };
            if media_type != MediaType::Audio || media_subtype != MediaSubtype::Raw {
                return;
            }
            if user_data.negotiated.parse(param).is_err() {
                return;
            }

            let format = AudioFormat::new(
                user_data.negotiated.rate(),
                user_data.negotiated.channels(),
                SampleFormat::F32Le,
            );
            user_data.format = Some(format);
            let _ = user_data
                .events
                .send(CaptureEvent::FormatNegotiated(format));
        })
        .process(|stream, user_data| {
            let Some(format) = user_data.format else {
                return;
            };
            let Some(mut buffer) = stream.dequeue_buffer() else {
                return;
            };
            let datas = buffer.datas_mut();
            let Some(data) = datas.first_mut() else {
                return;
            };
            let (offset, size) = {
                let chunk = data.chunk();
                (chunk.offset(), chunk.size())
            };
            let Some(bytes) = data.data() else {
                return;
            };
            let payload = copy_chunk(bytes, offset, size);
            if payload.is_empty() {
                return;
            }

            let nanos = user_data.session_start.elapsed().as_nanos();
            let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);
            let sequence = user_data.sequence;
            user_data.sequence = user_data.sequence.saturating_add(1);
            let frame = AudioFrame::new(
                sequence,
                MonotonicTimestamp::from_nanos(nanos),
                format,
                payload,
            );

            if let Err(error) = user_data.frames.try_send(frame) {
                let _ = user_data
                    .events
                    .send(CaptureEvent::Error(error.to_string()));
            }
        })
        .register()
        .map_err(|error| CaptureError::Backend(error.to_string()))?;

    let mut audio_info = spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(spa::param::audio::AudioFormat::F32LE);
    let format_object = spa::pod::Object {
        type_: spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: spa::param::ParamType::EnumFormat.as_raw(),
        properties: audio_info.into(),
    };
    let values = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(format_object),
    )
    .map_err(|error| CaptureError::Backend(format!("failed to serialize audio format: {error:?}")))?
    .0
    .into_inner();
    let param = Pod::from_bytes(&values)
        .map_err(|error| CaptureError::Backend(format!("invalid audio format pod: {error:?}")))?;
    let mut params = [param];

    stream
        .connect(
            spa::utils::Direction::Input,
            None,
            pw::stream::StreamFlags::AUTOCONNECT
                | pw::stream::StreamFlags::MAP_BUFFERS
                | pw::stream::StreamFlags::DONT_RECONNECT,
            &mut params,
        )
        .map_err(|error| CaptureError::Backend(error.to_string()))?;

    let (control_tx, control_rx) = pw::channel::channel::<()>();
    let _attached_control = control_rx.attach(mainloop.loop_(), {
        let mainloop = mainloop.clone();
        move |_| mainloop.quit()
    });

    let watcher_token = cancellation.clone();
    let watcher = thread::spawn(move || {
        while !watcher_token.is_cancelled() {
            thread::sleep(Duration::from_millis(20));
        }
        let _ = control_tx.send(());
    });

    mainloop.run();

    let state_after_loop = stream.state();
    cancellation.cancel();
    let _ = watcher.join();
    if !matches!(state_after_loop, pw::stream::StreamState::Unconnected) {
        let _ = stream.disconnect();
    }

    let reason = if matches!(state_after_loop, pw::stream::StreamState::Error(_)) {
        StopReason::BackendError
    } else if matches!(state_after_loop, pw::stream::StreamState::Unconnected) {
        StopReason::SourceLost
    } else {
        StopReason::Cancelled
    };

    let _ = events.send(CaptureEvent::Stopped(reason));
    Ok(reason)
}

fn copy_chunk(bytes: &[u8], offset: u32, size: u32) -> Vec<u8> {
    if bytes.is_empty() || size == 0 {
        return Vec::new();
    }

    let max_size = bytes.len();
    let start = (offset as usize) % max_size;
    let size = (size as usize).min(max_size);
    let first_len = size.min(max_size - start);

    let mut payload = Vec::with_capacity(size);
    payload.extend_from_slice(&bytes[start..start + first_len]);
    if first_len < size {
        payload.extend_from_slice(&bytes[..size - first_len]);
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::copy_chunk;

    #[test]
    fn copy_chunk_honors_offset_and_size() {
        assert_eq!(copy_chunk(&[0, 1, 2, 3, 4], 2, 2), vec![2, 3]);
    }

    #[test]
    fn copy_chunk_clamps_size_and_handles_wrapped_offsets() {
        assert_eq!(copy_chunk(&[0, 1, 2, 3], 3, 4), vec![3, 0, 1, 2]);
        assert_eq!(copy_chunk(&[0, 1, 2, 3], 9, 2), vec![1, 2]);
    }
}
