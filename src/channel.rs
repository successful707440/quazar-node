use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::HeaderMap,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json, Response,
    },
};
use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, RwLock};

use crate::auth::KeyStore;
use crate::chat::{
    search_db, send_message_as_bot, ChatMessageRow, DbSearchResult, SearchScope,
    DEFAULT_SEARCH_LIMIT, SYSTEM_BOT_ID, SYSTEM_BOT_NAME,
};
use crate::response::{self, ApiResponse};
use crate::AppState;

const DEFAULT_CHANNEL_ID: &str = "quazar_chat";
const BROADCAST_CAPACITY: usize = 512;
const MAX_DEDUP_ENTRIES: usize = 10_000;

#[derive(Debug, Clone, Serialize)]
struct WsFrame {
    #[serde(rename = "type")]
    frame_type: String,
    channelId: String,
    timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct FrontendChatEvent {
    #[serde(rename = "type")]
    event_type: String,
    id: String,
    citizen_id: String,
    citizen_name: String,
    content: String,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    crossChannel: Option<CrossChannelMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossChannelMeta {
    pub sourceChannel: String,
    pub direction: String,
}

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    #[serde(rename = "channelId")]
    pub channel_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(rename = "channelId")]
    pub channel_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct ConnectRequest {
    pub channelId: String,
    #[allow(dead_code)]
    pub pluginVersion: Option<String>,
    #[allow(dead_code)]
    pub workingDir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DisconnectRequest {
    pub channelId: String,
}

#[derive(Debug, Deserialize)]
pub struct ChannelMessageRequest {
    pub messageId: Option<String>,
    pub target: Option<TargetRef>,
    pub content: Option<MessageContentRef>,
    #[allow(dead_code)]
    pub role: Option<String>,
    #[allow(dead_code)]
    pub timestamp: Option<i64>,
    #[allow(dead_code)]
    pub replyTo: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct CrossChannelRequest {
    pub sourceChannel: String,
    pub direction: String,
    pub sender: CrossChannelSender,
    pub content: String,
    #[allow(dead_code)]
    pub sessionKey: Option<String>,
    #[serde(default)]
    pub dedupId: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CrossChannelSender {
    pub id: Option<String>,
    pub name: String,
}

#[derive(Debug, Deserialize)]
struct TargetRef {
    #[allow(dead_code)]
    #[serde(rename = "type")]
    target_type: Option<String>,
    id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageContentRef {
    text: Option<String>,
    #[allow(dead_code)]
    format: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChannelMessageResponse {
    status: String,
    messageId: String,
    data: Value,
}

#[derive(Debug, Clone, Serialize)]
struct CrossChannelResponse {
    id: String,
    channelId: String,
    createdAt: String,
}

pub struct ChannelHub {
    plugin_senders: RwLock<HashMap<String, broadcast::Sender<String>>>,
    frontend_senders: RwLock<HashMap<String, broadcast::Sender<String>>>,
    plugin_connections: RwLock<HashMap<String, usize>>,
    dedup_cache: RwLock<HashMap<String, CrossChannelResponse>>,
}

impl ChannelHub {
    pub fn new() -> Self {
        Self {
            plugin_senders: RwLock::new(HashMap::new()),
            frontend_senders: RwLock::new(HashMap::new()),
            plugin_connections: RwLock::new(HashMap::new()),
            dedup_cache: RwLock::new(HashMap::new()),
        }
    }

    async fn plugin_subscribe(&self, channel_id: &str) -> broadcast::Receiver<String> {
        let mut senders = self.plugin_senders.write().await;
        if let Some(sender) = senders.get(channel_id) {
            return sender.subscribe();
        }
        let (tx, rx) = broadcast::channel(BROADCAST_CAPACITY);
        senders.insert(channel_id.to_string(), tx);
        rx
    }

    async fn frontend_subscribe(&self, channel_id: &str) -> broadcast::Receiver<String> {
        let mut senders = self.frontend_senders.write().await;
        if let Some(sender) = senders.get(channel_id) {
            return sender.subscribe();
        }
        let (tx, rx) = broadcast::channel(BROADCAST_CAPACITY);
        senders.insert(channel_id.to_string(), tx);
        rx
    }

    async fn register_plugin(&self, channel_id: &str) {
        let mut connections = self.plugin_connections.write().await;
        *connections.entry(channel_id.to_string()).or_insert(0) += 1;
    }

    async fn unregister_plugin(&self, channel_id: &str) {
        let mut connections = self.plugin_connections.write().await;
        if let Some(count) = connections.get_mut(channel_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                connections.remove(channel_id);
            }
        }
    }

    pub async fn is_plugin_connected(&self, channel_id: &str) -> bool {
        self.plugin_connections
            .read()
            .await
            .get(channel_id)
            .copied()
            .unwrap_or(0)
            > 0
    }

    pub async fn push_plugin_json(&self, channel_id: &str, payload: String) {
        let sender = {
            let senders = self.plugin_senders.read().await;
            senders.get(channel_id).cloned()
        };
        if let Some(tx) = sender {
            let _ = tx.send(payload);
        }
    }

    pub async fn push_frontend_json(&self, channel_id: &str, payload: String) {
        let sender = {
            let senders = self.frontend_senders.read().await;
            senders.get(channel_id).cloned()
        };
        if let Some(tx) = sender {
            let _ = tx.send(payload);
        }
    }

    async fn remember_dedup(&self, dedup_id: String, response: CrossChannelResponse) {
        let mut cache = self.dedup_cache.write().await;
        if cache.len() >= MAX_DEDUP_ENTRIES {
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
        }
        cache.insert(dedup_id, response);
    }

    async fn lookup_dedup(&self, dedup_id: &str) -> Option<CrossChannelResponse> {
        self.dedup_cache.read().await.get(dedup_id).cloned()
    }
}

pub fn configured_channel_id() -> String {
    std::env::var("QUAZAR_CHATU_CHANNEL_ID").unwrap_or_else(|_| DEFAULT_CHANNEL_ID.to_string())
}

fn channel_access_token() -> String {
    std::env::var("ADMIN_API_KEY").unwrap_or_default()
}

fn validate_channel_admin(channel_id: &str, token: &str) -> bool {
    let expected_channel = configured_channel_id();
    let expected_token = channel_access_token();
    !expected_token.is_empty()
        && token == expected_token
        && channel_id == expected_channel
}

fn extract_access_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-access-token")
        .or_else(|| headers.get("X-Access-Token"))
        .or_else(|| headers.get("X-Channel-Token"))
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or_else(|| {
            headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(String::from)
        })
}

fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn is_valid_source_channel(source: &str) -> bool {
    let len = source.len();
    (1..=64).contains(&len)
        && source
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// TUI/CLI sessions are relayed for logging but must not appear in the web chat.
fn is_tui_source_channel(source: &str) -> bool {
    source == "tui"
}

pub fn is_tui_chat_message(message: &ChatMessageRow) -> bool {
    message.citizen_id == "cross-tui"
        || message.citizen_name == "tui"
        || message
            .citizen_name
            .strip_suffix(" [tui]")
            .is_some_and(|_| message.citizen_id == SYSTEM_BOT_ID)
}

fn build_plugin_frame(channel_id: &str, message: &ChatMessageRow) -> String {
    build_plugin_frame_with_context(channel_id, message, &[])
}

fn format_plugin_message_text(message: &ChatMessageRow, context: &[DbSearchResult]) -> String {
    if context.is_empty() {
        return message.content.clone();
    }

    let mut parts = vec!["[Контекст из реестра]".to_string()];
    for item in context {
        let stamp = item
            .created_at
            .map(|ts| ts.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "—".to_string());
        parts.push(format!(
            "{} [{}] ({}): {}",
            item.title, item.source, stamp, item.content
        ));
    }
    parts.push(String::new());
    parts.push("[Вопрос]".to_string());
    parts.push(message.content.clone());
    parts.join("\n")
}

fn build_plugin_frame_with_context(
    channel_id: &str,
    message: &ChatMessageRow,
    context: &[DbSearchResult],
) -> String {
    let text = format_plugin_message_text(message, context);
    let frame = WsFrame {
        frame_type: "message".to_string(),
        channelId: channel_id.to_string(),
        timestamp: now_millis(),
        payload: Some(json!({
            "id": message.id,
            "channelId": channel_id,
            "sender": {
                "id": message.citizen_id,
                "displayName": message.citizen_name,
                "isBot": message.citizen_id == SYSTEM_BOT_ID
            },
            "target": {
                "type": "user",
                "id": SYSTEM_BOT_ID
            },
            "content": {
                "text": text,
                "format": "plain"
            },
            "historyContext": context.iter().map(|item| json!({
                "id": item.id,
                "title": item.title,
                "source": item.source,
                "content": item.content,
                "created_at": item.created_at.map(|ts| ts.to_rfc3339()),
            })).collect::<Vec<_>>(),
            "timestamp": message.created_at.timestamp_millis()
        })),
    };
    serde_json::to_string(&frame).unwrap_or_default()
}

fn build_frontend_event(
    message: &ChatMessageRow,
    cross_channel: Option<CrossChannelMeta>,
) -> String {
    let event = FrontendChatEvent {
        event_type: "chat.message".to_string(),
        id: message.id.clone(),
        citizen_id: message.citizen_id.clone(),
        citizen_name: message.citizen_name.clone(),
        content: message.content.clone(),
        created_at: message.created_at.to_rfc3339(),
        crossChannel: cross_channel,
    };
    serde_json::to_string(&event).unwrap_or_default()
}

pub async fn broadcast_chat_message(
    state: &Arc<AppState>,
    message: &ChatMessageRow,
    cross_channel: Option<CrossChannelMeta>,
    plugin_context: Option<&[DbSearchResult]>,
) {
    let channel_id = configured_channel_id();

    if message.citizen_id != SYSTEM_BOT_ID && cross_channel.is_none() {
        let payload = if let Some(context) = plugin_context {
            build_plugin_frame_with_context(&channel_id, message, context)
        } else {
            build_plugin_frame(&channel_id, message)
        };
        state.channel_hub.push_plugin_json(&channel_id, payload).await;
    }

    if cross_channel
        .as_ref()
        .is_some_and(|m| is_tui_source_channel(&m.sourceChannel))
        || is_tui_chat_message(message)
    {
        return;
    }

    let frontend_payload = build_frontend_event(message, cross_channel);
    state
        .channel_hub
        .push_frontend_json(&channel_id, frontend_payload)
        .await;
}

pub async fn notify_user_message(state: &Arc<AppState>, message: &ChatMessageRow) {
    if message.citizen_id == SYSTEM_BOT_ID {
        return;
    }

    let context = match search_db(
        &state.db,
        &message.content,
        SearchScope::All,
        DEFAULT_SEARCH_LIMIT,
    )
    .await
    {
        Ok(rows) => rows
            .into_iter()
            .filter(|row| row.source != "chat" || row.id != message.id)
            .collect::<Vec<_>>(),
        Err(e) => {
            tracing::warn!(error = %e, message_id = %message.id, "registry search failed");
            Vec::new()
        }
    };

    broadcast_chat_message(state, message, None, Some(&context)).await;
    tracing::info!(
        channel_id = %configured_channel_id(),
        message_id = %message.id,
        citizen = %message.citizen_name,
        context_hits = context.len(),
        "chatu: pushed user message to plugin and frontend"
    );
}

pub async fn notify_chat_message(
    state: &Arc<AppState>,
    message: &ChatMessageRow,
    cross_channel: Option<CrossChannelMeta>,
) {
    broadcast_chat_message(state, message, cross_channel, None).await;
}

async fn validate_events_access(
    state: &Arc<AppState>,
    channel_id: &str,
    token: &str,
) -> bool {
    if validate_channel_admin(channel_id, token) {
        return true;
    }

    KeyStore::validate_key(&state.db, token)
        .await
        .is_some()
}

pub async fn health_handler() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339()
    }))
}

pub async fn status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let token = match extract_access_token(&headers) {
        Some(t) => t,
        None => return response::unauthorized("Missing access token"),
    };
    let channel_id = configured_channel_id();
    if !validate_channel_admin(&channel_id, &token) {
        return response::unauthorized("Invalid channel token");
    }

    let connected = state.channel_hub.is_plugin_connected(&channel_id).await;
    Json(ApiResponse::success(json!({
        "status": if connected { "connected" } else { "disconnected" },
        "channelId": channel_id
    })))
    .into_response()
}

pub async fn connect_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ConnectRequest>,
) -> impl IntoResponse {
    let token = match extract_access_token(&headers) {
        Some(t) => t,
        None => return response::unauthorized("Missing access token"),
    };
    if !validate_channel_admin(&req.channelId, &token) {
        return response::unauthorized("Invalid channel token");
    }

    tracing::info!(channel_id = %req.channelId, "chatu: plugin connected");
    let connected = state.channel_hub.is_plugin_connected(&req.channelId).await;
    Json(ApiResponse::success(json!({
        "status": if connected { "connected" } else { "registered" },
        "channelId": req.channelId
    })))
    .into_response()
}

pub async fn disconnect_handler(
    headers: HeaderMap,
    Json(req): Json<DisconnectRequest>,
) -> impl IntoResponse {
    let token = match extract_access_token(&headers) {
        Some(t) => t,
        None => return response::unauthorized("Missing access token"),
    };
    if !validate_channel_admin(&req.channelId, &token) {
        return response::unauthorized("Invalid channel token");
    }

    tracing::info!(channel_id = %req.channelId, "chatu: plugin disconnected");
    Json(ApiResponse::success(json!({ "status": "disconnected" }))).into_response()
}

pub async fn post_message_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ChannelMessageRequest>,
) -> impl IntoResponse {
    let token = match extract_access_token(&headers) {
        Some(t) => t,
        None => return response::unauthorized("Missing access token"),
    };
    let channel_id = headers
        .get("X-Channel-ID")
        .or_else(|| headers.get("x-channel-id"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&configured_channel_id())
        .to_string();

    if !validate_channel_admin(&channel_id, &token) {
        return response::unauthorized("Invalid channel token");
    }

    let text = req
        .content
        .as_ref()
        .and_then(|c| c.text.as_deref())
        .unwrap_or("")
        .trim()
        .to_string();
    if text.is_empty() {
        return response::bad_request("Message text is required");
    }

    match send_message_as_bot(&state, &text).await {
        Ok(row) => {
            notify_chat_message(&state, &row, None).await;
            tracing::info!(
                channel_id = %channel_id,
                message_id = %row.id,
                target = ?req.target.as_ref().and_then(|t| t.id.clone()),
                "chatu: AI reply stored and broadcast"
            );
            let stored_id = row.id.clone();
            Json(ChannelMessageResponse {
                status: "success".to_string(),
                messageId: stored_id.clone(),
                data: json!({ "messageId": stored_id }),
            })
            .into_response()
        }
        Err(e) => response::bad_request(e.to_string()),
    }
}

pub async fn cross_channel_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CrossChannelRequest>,
) -> impl IntoResponse {
    let token = match extract_access_token(&headers) {
        Some(t) => t,
        None => return response::unauthorized("Missing access token"),
    };
    let channel_id = configured_channel_id();
    if !validate_channel_admin(&channel_id, &token) {
        return response::unauthorized("Invalid channel token");
    }

    if !is_valid_source_channel(&req.sourceChannel) {
        return response::bad_request("Invalid sourceChannel");
    }
    if is_tui_source_channel(&req.sourceChannel) {
        tracing::debug!(source = %req.sourceChannel, "chatu: tui cross-channel message ignored for web chat");
        return Json(CrossChannelResponse {
            id: req
                .dedupId
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            channelId: channel_id,
            createdAt: Utc::now().to_rfc3339(),
        })
        .into_response();
    }
    if req.direction != "inbound" && req.direction != "outbound" {
        return response::bad_request("direction must be inbound or outbound");
    }
    let content = req.content.trim();
    if content.is_empty() {
        return response::bad_request("content is required");
    }
    if req.sender.name.trim().is_empty() {
        return response::bad_request("sender.name is required");
    }

    if let Some(dedup_id) = req.dedupId.as_deref().filter(|s| !s.is_empty()) {
        if let Some(existing) = state.channel_hub.lookup_dedup(dedup_id).await {
            return Json(existing).into_response();
        }
    }

    let cross_meta = CrossChannelMeta {
        sourceChannel: req.sourceChannel.clone(),
        direction: req.direction.clone(),
    };

    let row = if req.direction == "inbound" {
        let display_name = format!("{} [{}]", SYSTEM_BOT_NAME, req.sourceChannel);
        insert_cross_channel_message(
            &state,
            SYSTEM_BOT_ID,
            &display_name,
            content,
        )
        .await
    } else {
        let citizen_id = req
            .sender
            .id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("cross-{}", req.sourceChannel));
        insert_cross_channel_message(&state, &citizen_id, &req.sender.name, content).await
    };

    let row = match row {
        Ok(row) => row,
        Err(e) => return response::bad_request(e),
    };

    notify_chat_message(&state, &row, Some(cross_meta)).await;

    let response_body = CrossChannelResponse {
        id: row.id.clone(),
        channelId: channel_id,
        createdAt: row.created_at.to_rfc3339(),
    };

    if let Some(dedup_id) = req.dedupId.filter(|s| !s.is_empty()) {
        state
            .channel_hub
            .remember_dedup(dedup_id, response_body.clone())
            .await;
    }

    tracing::info!(
        message_id = %row.id,
        source = %req.sourceChannel,
        direction = %req.direction,
        "chatu: cross-channel message stored"
    );

    Json(response_body).into_response()
}

async fn ensure_channel_citizen(
    pool: &sqlx::PgPool,
    citizen_id: &str,
    citizen_name: &str,
) -> Result<(), String> {
    let now = Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
         VALUES ($1, $2, $3, 'active', 'Citizen', $4, false)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(citizen_id)
    .bind(citizen_name)
    .bind("0000000000000000000000000000000000000000000000000000000000000000")
    .bind(now)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

async fn insert_cross_channel_message(
    state: &Arc<AppState>,
    citizen_id: &str,
    citizen_name: &str,
    content: &str,
) -> Result<ChatMessageRow, String> {
    if citizen_id != SYSTEM_BOT_ID {
        ensure_channel_citizen(&state.db, citizen_id, citizen_name).await?;
    } else {
        sqlx::query(
            "INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
             VALUES ($1, $2, $3, 'active', 'Citizen', $4, false)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(SYSTEM_BOT_ID)
        .bind(SYSTEM_BOT_NAME)
        .bind("0000000000000000000000000000000000000000000000000000000000000000")
        .bind(Utc::now().timestamp())
        .execute(&state.db)
        .await
        .map_err(|e| e.to_string())?;
    }

    let id = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO chat_messages (id, citizen_id, citizen_name, content, created_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(citizen_id)
    .bind(citizen_name)
    .bind(content)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| e.to_string())?;

    Ok(ChatMessageRow {
        id,
        citizen_id: citizen_id.to_string(),
        citizen_name: citizen_name.to_string(),
        content: content.to_string(),
        created_at: now,
    })
}

pub async fn events_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EventsQuery>,
) -> Response {
    if !validate_events_access(&state, &params.channel_id, &params.token).await {
        return response::unauthorized("Invalid channel credentials");
    }

    let channel_id = params.channel_id.clone();
    let rx = state.channel_hub.frontend_subscribe(&channel_id).await;

    let welcome = build_frontend_event(
        &ChatMessageRow {
            id: "sse-connected".to_string(),
            citizen_id: "system".to_string(),
            citizen_name: "Квазар".to_string(),
            content: String::new(),
            created_at: Utc::now(),
        },
        None,
    );

    let initial = stream::once(async move {
        Ok::<Event, Infallible>(Event::default().event("connected").data(welcome))
    });

    let channel_id_for_log = channel_id.clone();
    let live = stream::unfold(rx, move |mut rx| {
        let channel_id_for_log = channel_id_for_log.clone();
        async move {
        match rx.recv().await {
            Ok(data) => Some((
                Ok::<Event, Infallible>(Event::default().event("message").data(data)),
                rx,
            )),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(channel_id = %channel_id_for_log, skipped, "chatu: SSE client lagged");
                Some((
                    Ok(Event::default().comment(format!("lagged {skipped}"))),
                    rx,
                ))
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
        }
    });

    let sse_stream = futures_util::StreamExt::chain(initial, live);

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !validate_channel_admin(&params.channel_id, &params.token) {
        return response::unauthorized("Invalid channel credentials");
    }

    ws.on_upgrade(move |socket| handle_plugin_socket(socket, state, params.channel_id))
}

async fn handle_plugin_socket(mut socket: WebSocket, state: Arc<AppState>, channel_id: String) {
    state.channel_hub.register_plugin(&channel_id).await;
    let mut rx = state.channel_hub.plugin_subscribe(&channel_id).await;

    tracing::info!(channel_id = %channel_id, "chatu: WebSocket plugin connected");

    let welcome = WsFrame {
        frame_type: "open".to_string(),
        channelId: channel_id.clone(),
        timestamp: now_millis(),
        payload: Some(json!({ "status": "connected" })),
    };
    if socket
        .send(Message::Text(
            serde_json::to_string(&welcome).unwrap_or_default(),
        ))
        .await
        .is_err()
    {
        state.channel_hub.unregister_plugin(&channel_id).await;
        return;
    }

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(frame) = serde_json::from_str::<Value>(&text) {
                            let frame_type = frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            if frame_type == "heartbeat" {
                                let pong = WsFrame {
                                    frame_type: "heartbeat".to_string(),
                                    channelId: channel_id.clone(),
                                    timestamp: now_millis(),
                                    payload: None,
                                };
                                if socket.send(Message::Text(serde_json::to_string(&pong).unwrap_or_default())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            outgoing = rx.recv() => {
                match outgoing {
                    Ok(payload) => {
                        if socket.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(channel_id = %channel_id, skipped, "chatu: plugin lagged behind broadcast");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    state.channel_hub.unregister_plugin(&channel_id).await;
    tracing::info!(channel_id = %channel_id, "chatu: WebSocket plugin disconnected");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_channel_admin_checks_token_and_id() {
        std::env::set_var("ADMIN_API_KEY", "test_token");
        std::env::set_var("QUAZAR_CHATU_CHANNEL_ID", "quazar_chat");
        assert!(validate_channel_admin("quazar_chat", "test_token"));
        assert!(!validate_channel_admin("other", "test_token"));
        assert!(!validate_channel_admin("quazar_chat", "wrong"));
    }

    #[test]
    fn source_channel_validation() {
        assert!(is_valid_source_channel("tui"));
        assert!(is_valid_source_channel("whatsapp"));
        assert!(!is_valid_source_channel("Bad"));
        assert!(!is_valid_source_channel(""));
    }

    #[test]
    fn tui_messages_are_identified() {
        assert!(is_tui_source_channel("tui"));
        assert!(!is_tui_source_channel("whatsapp"));

        assert!(is_tui_chat_message(&ChatMessageRow {
            id: "1".to_string(),
            citizen_id: "cross-tui".to_string(),
            citizen_name: "tui".to_string(),
            content: "hello".to_string(),
            created_at: Utc::now(),
        }));
        assert!(is_tui_chat_message(&ChatMessageRow {
            id: "2".to_string(),
            citizen_id: SYSTEM_BOT_ID.to_string(),
            citizen_name: "Квазар [tui]".to_string(),
            content: "reply".to_string(),
            created_at: Utc::now(),
        }));
        assert!(!is_tui_chat_message(&ChatMessageRow {
            id: "3".to_string(),
            citizen_id: SYSTEM_BOT_ID.to_string(),
            citizen_name: "Квазар [whatsapp]".to_string(),
            content: "reply".to_string(),
            created_at: Utc::now(),
        }));
    }
}
