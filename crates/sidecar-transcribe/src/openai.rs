use crate::{
    f32le_to_pcm16_mono_24khz, ChannelSession, ProviderHealth, SpeakerMetadata, TranscriptSegment,
    TranscriptionError, TranscriptionEvent, TranscriptionProvider, TranscriptionSession,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sidecar_audio::CancellationToken;
use sidecar_core::{AudioFrame, MonotonicTimestamp};
use std::sync::mpsc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_ENDPOINT: &str = "wss://api.openai.com/v1/realtime";
const DEFAULT_MODEL: &str = "gpt-4o-mini-transcribe";
const AUDIO_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub struct OpenAiRealtimeConfig {
    pub api_key: String,
    pub endpoint: String,
    pub model: String,
    pub language: Option<String>,
    pub max_reconnects: u32,
}

impl OpenAiRealtimeConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, TranscriptionError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(TranscriptionError::Provider(
                "OpenAI API key must not be empty".to_string(),
            ));
        }
        Ok(Self {
            api_key,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            model: DEFAULT_MODEL.to_string(),
            language: None,
            max_reconnects: 3,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiRealtimeProvider {
    config: OpenAiRealtimeConfig,
}

impl OpenAiRealtimeProvider {
    pub fn new(config: OpenAiRealtimeConfig) -> Self {
        Self { config }
    }
}

impl TranscriptionProvider for OpenAiRealtimeProvider {
    fn connect(&self) -> Result<Box<dyn TranscriptionSession>, TranscriptionError> {
        let (audio_tx, audio_rx) = tokio::sync::mpsc::channel(AUDIO_QUEUE_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel();
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let config = self.config.clone();
        std::thread::Builder::new()
            .name("sidecar-openai-transcribe".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = event_tx.send(TranscriptionEvent::Error(format!(
                            "failed to start transcription runtime: {error}"
                        )));
                        return;
                    }
                };
                runtime.block_on(run_worker(config, audio_rx, event_tx, worker_cancellation));
            })
            .map_err(|error| {
                TranscriptionError::Provider(format!("failed to spawn provider worker: {error}"))
            })?;

        Ok(Box::new(ChannelSession {
            audio_tx,
            events_rx: std::sync::Mutex::new(event_rx),
            cancellation,
        }))
    }
}

async fn run_worker(
    config: OpenAiRealtimeConfig,
    mut audio_rx: tokio::sync::mpsc::Receiver<AudioFrame>,
    event_tx: mpsc::Sender<TranscriptionEvent>,
    cancellation: CancellationToken,
) {
    let mut attempt = 0_u32;
    loop {
        if cancellation.is_cancelled() {
            let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Stopped));
            return;
        }
        let _ = event_tx.send(TranscriptionEvent::Health(if attempt == 0 {
            ProviderHealth::Connecting
        } else {
            ProviderHealth::Reconnecting { attempt }
        }));

        match run_connection(&config, &mut audio_rx, &event_tx, &cancellation).await {
            Ok(()) => {
                let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Stopped));
                return;
            }
            Err(error) => {
                if cancellation.is_cancelled() {
                    let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Stopped));
                    return;
                }
                if attempt >= config.max_reconnects {
                    let _ = event_tx.send(TranscriptionEvent::Error(error.to_string()));
                    let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Degraded(
                        "reconnect budget exhausted".to_string(),
                    )));
                    return;
                }
                attempt = attempt.saturating_add(1);
                let delay_ms = 100_u64.saturating_mul(1_u64 << attempt.min(5));
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

async fn run_connection(
    config: &OpenAiRealtimeConfig,
    audio_rx: &mut tokio::sync::mpsc::Receiver<AudioFrame>,
    event_tx: &mpsc::Sender<TranscriptionEvent>,
    cancellation: &CancellationToken,
) -> Result<(), TranscriptionError> {
    let mut request = config
        .endpoint
        .as_str()
        .into_client_request()
        .map_err(|error| {
            TranscriptionError::Provider(format!("invalid realtime endpoint: {error}"))
        })?;
    let authorization =
        HeaderValue::from_str(&format!("Bearer {}", config.api_key)).map_err(|error| {
            TranscriptionError::Provider(format!("invalid authorization header: {error}"))
        })?;
    request.headers_mut().insert(AUTHORIZATION, authorization);

    let (websocket, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|error| {
            TranscriptionError::Provider(format!("realtime connect failed: {error}"))
        })?;
    let (mut write, mut read) = websocket.split();
    write
        .send(Message::Text(session_update(config).to_string().into()))
        .await
        .map_err(|error| TranscriptionError::Provider(format!("session update failed: {error}")))?;
    let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Healthy));

    let mut latest_audio = MonotonicTimestamp::ZERO;
    let mut last_final_end = MonotonicTimestamp::ZERO;
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if cancellation.is_cancelled() {
                    let _ = write.close().await;
                    return Ok(());
                }
            }
            frame = audio_rx.recv() => {
                let Some(frame) = frame else {
                    let _ = write.close().await;
                    return Ok(());
                };
                latest_audio = frame.timestamp;
                let pcm = f32le_to_pcm16_mono_24khz(&frame)?;
                if pcm.is_empty() {
                    continue;
                }
                let payload = json!({
                    "type": "input_audio_buffer.append",
                    "audio": BASE64_STANDARD.encode(pcm),
                });
                write
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .map_err(|error| TranscriptionError::Provider(format!("audio send failed: {error}")))?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    return Err(TranscriptionError::Provider("realtime connection closed".to_string()));
                };
                let message = message
                    .map_err(|error| TranscriptionError::Provider(format!("realtime read failed: {error}")))?;
                match message {
                    Message::Text(text) => {
                        if let Some(event) = parse_server_event(
                            text.as_str(),
                            last_final_end,
                            latest_audio,
                        )? {
                            if let TranscriptionEvent::Final(segment) = &event {
                                last_final_end = segment.end;
                            }
                            let _ = event_tx.send(event);
                        }
                    }
                    Message::Close(_) => {
                        return Err(TranscriptionError::Provider("realtime server closed the connection".to_string()));
                    }
                    Message::Ping(payload) => {
                        write
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|error| TranscriptionError::Provider(format!("pong failed: {error}")))?;
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn session_update(config: &OpenAiRealtimeConfig) -> Value {
    let mut transcription = json!({ "model": config.model });
    if let Some(language) = &config.language {
        transcription["language"] = Value::String(language.clone());
    }
    json!({
        "type": "session.update",
        "session": {
            "type": "transcription",
            "audio": {
                "input": {
                    "format": { "type": "audio/pcm", "rate": 24000 },
                    "transcription": transcription,
                    "turn_detection": { "type": "server_vad" }
                }
            }
        }
    })
}

fn parse_server_event(
    payload: &str,
    fallback_start: MonotonicTimestamp,
    fallback_end: MonotonicTimestamp,
) -> Result<Option<TranscriptionEvent>, TranscriptionError> {
    let value: Value = serde_json::from_str(payload)
        .map_err(|error| TranscriptionError::Protocol(format!("invalid provider JSON: {error}")))?;
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| TranscriptionError::Protocol("provider event missing type".to_string()))?;
    match event_type {
        "conversation.item.input_audio_transcription.delta" => {
            let id = required_string(&value, "item_id")?;
            let delta = required_string(&value, "delta")?;
            if delta.is_empty() {
                return Ok(None);
            }
            Ok(Some(TranscriptionEvent::Partial {
                provider_segment_id: id,
                text: delta,
                speaker: parse_speaker(&value),
            }))
        }
        "conversation.item.input_audio_transcription.completed" => {
            let id = required_string(&value, "item_id")?;
            let transcript = required_string(&value, "transcript")?;
            if transcript.trim().is_empty() {
                return Ok(None);
            }
            let start_ms = value.get("audio_start_ms").and_then(Value::as_u64);
            let end_ms = value.get("audio_end_ms").and_then(Value::as_u64);
            let start = start_ms
                .map(|ms| MonotonicTimestamp::from_nanos(ms.saturating_mul(1_000_000)))
                .unwrap_or(fallback_start);
            let mut end = end_ms
                .map(|ms| MonotonicTimestamp::from_nanos(ms.saturating_mul(1_000_000)))
                .unwrap_or(fallback_end);
            if end < start {
                end = start;
            }
            let segment =
                TranscriptSegment::new(id, transcript, start, end, parse_speaker(&value))?;
            Ok(Some(TranscriptionEvent::Final(segment)))
        }
        "conversation.item.input_audio_transcription.failed" | "error" => {
            let message = value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .or_else(|| value.get("message").and_then(Value::as_str))
                .unwrap_or("unknown transcription provider error");
            Ok(Some(TranscriptionEvent::Error(message.to_string())))
        }
        "session.created"
        | "session.updated"
        | "transcription_session.created"
        | "transcription_session.updated" => {
            Ok(Some(TranscriptionEvent::Health(ProviderHealth::Healthy)))
        }
        _ => Ok(None),
    }
}

fn required_string(value: &Value, key: &str) -> Result<String, TranscriptionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| TranscriptionError::Protocol(format!("provider event missing {key}")))
}

fn parse_speaker(value: &Value) -> Option<SpeakerMetadata> {
    let speaker = value.get("speaker")?;
    match speaker {
        Value::String(label) => Some(SpeakerMetadata {
            id: label.clone(),
            label: Some(label.clone()),
        }),
        Value::Object(map) => {
            let id = map.get("id")?.as_str()?.to_string();
            let label = map
                .get("label")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            Some(SpeakerMetadata { id, label })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_update_uses_transcription_mode_and_pcm24k() {
        let config = OpenAiRealtimeConfig::new("test-key").unwrap();
        let value = session_update(&config);
        assert_eq!(value["session"]["type"], "transcription");
        assert_eq!(
            value["session"]["audio"]["input"]["format"]["rate"],
            24_000
        );
        assert_eq!(
            value["session"]["audio"]["input"]["transcription"]["model"],
            DEFAULT_MODEL
        );
    }

    #[test]
    fn parses_partial_and_final_events_without_provider_types_leaking() {
        let partial = parse_server_event(
            r#"{"type":"conversation.item.input_audio_transcription.delta","item_id":"i1","delta":"hel"}"#,
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::from_nanos(5),
        )
        .unwrap();
        assert!(matches!(partial, Some(TranscriptionEvent::Partial { text, .. }) if text == "hel"));

        let final_event = parse_server_event(
            r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"i1","transcript":"hello"}"#,
            MonotonicTimestamp::from_nanos(2),
            MonotonicTimestamp::from_nanos(8),
        )
        .unwrap();
        assert!(
            matches!(final_event, Some(TranscriptionEvent::Final(segment)) if segment.text == "hello" && segment.start.nanos_since_start == 2 && segment.end.nanos_since_start == 8)
        );
    }

    #[test]
    fn provider_errors_remain_observable() {
        let event = parse_server_event(
            r#"{"type":"error","error":{"message":"bad audio"}}"#,
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::ZERO,
        )
        .unwrap();
        assert_eq!(
            event,
            Some(TranscriptionEvent::Error("bad audio".to_string()))
        );
    }
}
