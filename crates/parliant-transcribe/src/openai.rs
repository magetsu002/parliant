use crate::{
    f32le_to_pcm16_mono_24khz, ChannelSession, ProviderHealth, SpeakerMetadata, TranscriptSegment,
    TranscriptionError, TranscriptionEvent, TranscriptionProvider, TranscriptionSession,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use parliant_audio::CancellationToken;
use parliant_core::{AudioFrame, MonotonicTimestamp};
use serde_json::{json, Value};
use std::sync::mpsc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_ENDPOINT: &str = "wss://api.openai.com/v1/realtime";
const DEFAULT_REALTIME_MODEL: &str = "gpt-realtime-2.1";
const DEFAULT_TRANSCRIPTION_MODEL: &str = "gpt-live-transcribe";
const AUDIO_QUEUE_CAPACITY: usize = 64;
const TARGET_AUDIO_CHUNK_BYTES: usize = 4_800;
const AUDIO_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const SESSION_READY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct OpenAiRealtimeConfig {
    pub api_key: String,
    pub endpoint: String,
    pub realtime_model: String,
    pub transcription_model: String,
    pub languages: Vec<String>,
    pub max_reconnects: u32,
}

impl OpenAiRealtimeConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, TranscriptionError> {
        let config = Self {
            api_key: api_key.into(),
            endpoint: DEFAULT_ENDPOINT.to_string(),
            realtime_model: DEFAULT_REALTIME_MODEL.to_string(),
            transcription_model: DEFAULT_TRANSCRIPTION_MODEL.to_string(),
            languages: Vec::new(),
            max_reconnects: 3,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), TranscriptionError> {
        if self.api_key.trim().is_empty() {
            return Err(TranscriptionError::Provider(
                "OpenAI API key must not be empty".to_string(),
            ));
        }
        validate_model_id("Realtime session model", &self.realtime_model)?;
        validate_model_id("transcription model", &self.transcription_model)?;
        let endpoint = self.endpoint.trim();
        if !endpoint.starts_with("wss://") {
            return Err(TranscriptionError::Provider(
                "OpenAI Realtime endpoint must use wss://".to_string(),
            ));
        }
        if endpoint.contains('#') {
            return Err(TranscriptionError::Provider(
                "OpenAI Realtime endpoint must not contain a URL fragment".to_string(),
            ));
        }
        if endpoint
            .split_once('?')
            .map(|(_, query)| {
                query.split('&').any(|part| {
                    part.split_once('=')
                        .map(|(key, _)| key == "model")
                        .unwrap_or(part == "model")
                })
            })
            .unwrap_or(false)
        {
            return Err(TranscriptionError::Provider(
                "OpenAI Realtime endpoint must not contain a model query; configure realtime_model separately"
                    .to_string(),
            ));
        }
        for language in &self.languages {
            if language.trim().is_empty()
                || !language
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
            {
                return Err(TranscriptionError::Provider(
                    "OpenAI transcription language codes must contain only letters, digits, or '-'"
                        .to_string(),
                ));
            }
        }
        Ok(())
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
        self.config.validate()?;
        let (audio_tx, audio_rx) = tokio::sync::mpsc::channel(AUDIO_QUEUE_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel();
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let config = self.config.clone();
        std::thread::Builder::new()
            .name("parliant-openai-transcribe".to_string())
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
                let safe_message = safe_error_text(&error, &config.api_key);
                let _ = event_tx.send(TranscriptionEvent::Error(safe_message));
                if attempt >= config.max_reconnects {
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
    config.validate()?;
    let websocket_url = realtime_websocket_url(config)?;
    let mut request = websocket_url
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

    tokio::time::timeout(SESSION_READY_TIMEOUT, async {
        loop {
            if cancellation.is_cancelled() {
                return Err(TranscriptionError::SessionClosed);
            }
            let message = read.next().await.ok_or_else(|| {
                TranscriptionError::Provider(
                    "realtime connection closed before session confirmation".to_string(),
                )
            })?;
            let message = message.map_err(|error| {
                TranscriptionError::Provider(format!("realtime read failed: {error}"))
            })?;
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_str()).map_err(|error| {
                        TranscriptionError::Protocol(format!("invalid provider JSON: {error}"))
                    })?;
                    match value.get("type").and_then(Value::as_str) {
                        Some("session.updated") => return Ok(()),
                        Some("error") | Some("conversation.item.input_audio_transcription.failed") => {
                            return Err(TranscriptionError::Provider(provider_error_message(&value)))
                        }
                        _ => {}
                    }
                }
                Message::Close(_) => {
                    return Err(TranscriptionError::Provider(
                        "realtime server closed before session confirmation".to_string(),
                    ));
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await.map_err(|error| {
                        TranscriptionError::Provider(format!("pong failed: {error}"))
                    })?;
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    })
    .await
    .map_err(|_| {
        TranscriptionError::Provider(
            "timed out waiting for OpenAI Realtime session.updated after transcription session.update"
                .to_string(),
        )
    })??;

    let _ = event_tx.send(TranscriptionEvent::Health(ProviderHealth::Healthy));

    let mut latest_audio = MonotonicTimestamp::ZERO;
    let mut last_final_end = MonotonicTimestamp::ZERO;
    let mut pending_pcm = Vec::with_capacity(TARGET_AUDIO_CHUNK_BYTES);
    let mut cancellation_tick = tokio::time::interval(Duration::from_millis(25));
    let mut flush_tick = tokio::time::interval(AUDIO_FLUSH_INTERVAL);
    flush_tick.tick().await;
    loop {
        tokio::select! {
            _ = cancellation_tick.tick() => {
                if cancellation.is_cancelled() {
                    let _ = write.close().await;
                    return Ok(());
                }
            }
            _ = flush_tick.tick() => {
                if !pending_pcm.is_empty() {
                    let pcm = std::mem::take(&mut pending_pcm);
                    let payload = audio_append_payload(&pcm);
                    write
                        .send(Message::Text(payload.to_string().into()))
                        .await
                        .map_err(|error| TranscriptionError::Provider(format!("audio send failed: {error}")))?;
                }
            }
            frame = audio_rx.recv() => {
                let Some(frame) = frame else {
                    let _ = write.close().await;
                    return Ok(());
                };
                latest_audio = frame.timestamp;
                pending_pcm.extend_from_slice(&f32le_to_pcm16_mono_24khz(&frame)?);
                while pending_pcm.len() >= TARGET_AUDIO_CHUNK_BYTES {
                    let remainder = pending_pcm.split_off(TARGET_AUDIO_CHUNK_BYTES);
                    let pcm = std::mem::replace(&mut pending_pcm, remainder);
                    let payload = audio_append_payload(&pcm);
                    write
                        .send(Message::Text(payload.to_string().into()))
                        .await
                        .map_err(|error| TranscriptionError::Provider(format!("audio send failed: {error}")))?;
                }
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
                            match event {
                                TranscriptionEvent::Error(message) => {
                                    return Err(TranscriptionError::Provider(message));
                                }
                                TranscriptionEvent::Final(segment) => {
                                    last_final_end = segment.end;
                                    let _ = event_tx.send(TranscriptionEvent::Final(segment));
                                }
                                other => {
                                    let _ = event_tx.send(other);
                                }
                            }
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

fn validate_model_id(label: &str, model: &str) -> Result<(), TranscriptionError> {
    if model.trim().is_empty() {
        return Err(TranscriptionError::Provider(format!(
            "OpenAI {label} must not be empty"
        )));
    }
    if !model
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
    {
        return Err(TranscriptionError::Provider(format!(
            "OpenAI {label} contains invalid characters"
        )));
    }
    Ok(())
}

fn realtime_websocket_url(config: &OpenAiRealtimeConfig) -> Result<String, TranscriptionError> {
    config.validate()?;
    let endpoint = config.endpoint.trim();
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    Ok(format!(
        "{endpoint}{separator}model={}",
        config.realtime_model
    ))
}

fn session_update(config: &OpenAiRealtimeConfig) -> Value {
    let mut transcription = json!({ "model": config.transcription_model });
    if !config.languages.is_empty() {
        transcription["languages"] = json!(config.languages);
    }
    let turn_detection = if config.transcription_model == "gpt-realtime-whisper" {
        Value::Null
    } else {
        json!({
            "type": "server_vad",
            "threshold": 0.5,
            "prefix_padding_ms": 300,
            "silence_duration_ms": 500
        })
    };
    json!({
        "type": "session.update",
        "session": {
            "type": "transcription",
            "audio": {
                "input": {
                    "format": { "type": "audio/pcm", "rate": 24000 },
                    "transcription": transcription,
                    "turn_detection": turn_detection
                }
            }
        }
    })
}

fn audio_append_payload(pcm: &[u8]) -> Value {
    json!({
        "type": "input_audio_buffer.append",
        "audio": BASE64_STANDARD.encode(pcm),
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
        "conversation.item.input_audio_transcription.failed" | "error" => Ok(Some(
            TranscriptionEvent::Error(provider_error_message(&value)),
        )),
        "session.created" | "session.updated" => {
            Ok(Some(TranscriptionEvent::Health(ProviderHealth::Healthy)))
        }
        _ => Ok(None),
    }
}

fn provider_error_message(value: &Value) -> String {
    value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
        .unwrap_or("unknown transcription provider error")
        .to_string()
}

fn safe_error_text(error: &TranscriptionError, api_key: &str) -> String {
    let message = match error {
        TranscriptionError::Provider(message) | TranscriptionError::Protocol(message) => {
            message.clone()
        }
        other => other.to_string(),
    };
    redact_credentials(&message, api_key)
}

fn redact_credentials(message: &str, api_key: &str) -> String {
    let mut redacted = if api_key.is_empty() {
        message.to_string()
    } else {
        message.replace(api_key, "[REDACTED]")
    };
    loop {
        let lowercase = redacted.to_ascii_lowercase();
        let Some(marker) = lowercase.find("bearer ") else {
            break;
        };
        let token_start = marker + "bearer ".len();
        let token_end = redacted[token_start..]
            .find(|ch: char| ch.is_ascii_whitespace() || matches!(ch, '"' | '\'' | ',' | ';'))
            .map(|offset| token_start + offset)
            .unwrap_or(redacted.len());
        if token_start == token_end || redacted[token_start..token_end] == "[REDACTED]" {
            break;
        }
        redacted.replace_range(token_start..token_end, "[REDACTED]");
    }
    redacted
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
    fn websocket_url_includes_realtime_model_and_keeps_models_separate() {
        let config = OpenAiRealtimeConfig::new("test-key").unwrap();
        assert_eq!(
            realtime_websocket_url(&config).unwrap(),
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1"
        );
        assert_eq!(config.realtime_model, DEFAULT_REALTIME_MODEL);
        assert_eq!(config.transcription_model, DEFAULT_TRANSCRIPTION_MODEL);
        assert_ne!(config.realtime_model, config.transcription_model);
    }

    #[test]
    fn websocket_query_construction_is_safe_and_deterministic() {
        let mut config = OpenAiRealtimeConfig::new("test-key").unwrap();
        config.endpoint = "wss://api.openai.com/v1/realtime?trace=1".to_string();
        assert_eq!(
            realtime_websocket_url(&config).unwrap(),
            "wss://api.openai.com/v1/realtime?trace=1&model=gpt-realtime-2.1"
        );
        config.realtime_model = "gpt-realtime-2.1&leak=1".to_string();
        assert!(realtime_websocket_url(&config).is_err());
    }

    #[test]
    fn session_update_uses_current_transcription_mode_pcm24k_and_server_vad() {
        let config = OpenAiRealtimeConfig::new("test-key").unwrap();
        let value = session_update(&config);
        assert_eq!(value["type"], "session.update");
        assert_eq!(value["session"]["type"], "transcription");
        assert_eq!(value["session"]["audio"]["input"]["format"]["type"], "audio/pcm");
        assert_eq!(value["session"]["audio"]["input"]["format"]["rate"], 24_000);
        assert_eq!(
            value["session"]["audio"]["input"]["transcription"]["model"],
            DEFAULT_TRANSCRIPTION_MODEL
        );
        assert_eq!(
            value["session"]["audio"]["input"]["turn_detection"],
            json!({
                "type": "server_vad",
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": 500
            })
        );
    }

    #[test]
    fn live_transcription_languages_use_current_plural_field() {
        let mut config = OpenAiRealtimeConfig::new("test-key").unwrap();
        config.languages = vec!["en".to_string(), "fr".to_string()];
        let value = session_update(&config);
        let transcription = &value["session"]["audio"]["input"]["transcription"];
        assert_eq!(transcription["languages"], json!(["en", "fr"]));
        assert!(transcription.get("language").is_none());
    }

    #[test]
    fn realtime_whisper_disables_vad_as_required() {
        let mut config = OpenAiRealtimeConfig::new("test-key").unwrap();
        config.transcription_model = "gpt-realtime-whisper".to_string();
        let value = session_update(&config);
        assert!(value["session"]["audio"]["input"]["turn_detection"].is_null());
    }

    #[test]
    fn parses_partial_and_final_events_without_provider_types_leaking() {
        let partial = parse_server_event(
            r#"{"type":"conversation.item.input_audio_transcription.delta","item_id":"i1","content_index":0,"delta":"hel"}"#,
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::from_nanos(5),
        )
        .unwrap();
        assert!(matches!(partial, Some(TranscriptionEvent::Partial { text, .. }) if text == "hel"));

        let final_event = parse_server_event(
            r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"i1","content_index":0,"transcript":"hello"}"#,
            MonotonicTimestamp::from_nanos(2),
            MonotonicTimestamp::from_nanos(8),
        )
        .unwrap();
        assert!(
            matches!(final_event, Some(TranscriptionEvent::Final(segment)) if segment.text == "hello" && segment.start.nanos_since_start == 2 && segment.end.nanos_since_start == 8)
        );
    }

    #[test]
    fn provider_errors_and_session_health_remain_observable() {
        let error = parse_server_event(
            r#"{"type":"error","error":{"message":"bad audio"}}"#,
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::ZERO,
        )
        .unwrap();
        assert_eq!(error, Some(TranscriptionEvent::Error("bad audio".to_string())));

        let ready = parse_server_event(
            r#"{"type":"session.updated","session":{"type":"transcription"}}"#,
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::ZERO,
        )
        .unwrap();
        assert_eq!(ready, Some(TranscriptionEvent::Health(ProviderHealth::Healthy)));
    }

    #[test]
    fn empty_or_invalid_models_fail_before_connecting() {
        let mut config = OpenAiRealtimeConfig::new("test-key").unwrap();
        config.realtime_model = " ".to_string();
        assert!(OpenAiRealtimeProvider::new(config).connect().is_err());

        let mut config = OpenAiRealtimeConfig::new("test-key").unwrap();
        config.transcription_model = "bad?model".to_string();
        assert!(OpenAiRealtimeProvider::new(config).connect().is_err());
    }

    #[test]
    fn provider_error_sanitization_removes_api_key_and_bearer_token() {
        let api_key = "sk-test-secret-value";
        let error = TranscriptionError::Provider(format!(
            "request rejected; Authorization: Bearer {api_key}; provider said no"
        ));
        let safe = safe_error_text(&error, api_key);
        assert!(!safe.contains(api_key));
        assert!(!safe.contains("Bearer sk-"));
        assert!(safe.contains("provider said no"));
    }

    #[test]
    fn audio_append_payload_is_bounded_to_the_chunk_supplied() {
        let pcm = vec![0_u8; TARGET_AUDIO_CHUNK_BYTES];
        let value = audio_append_payload(&pcm);
        assert_eq!(value["type"], "input_audio_buffer.append");
        assert_eq!(
            BASE64_STANDARD.decode(value["audio"].as_str().unwrap()).unwrap(),
            pcm
        );
    }
}
