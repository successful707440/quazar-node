use std::sync::Arc;

use axum::{
    extract::{Extension, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::AuthContext;
use crate::response::{self, ApiResponse};
use crate::AppState;

pub const MAX_MESSAGE_LEN: usize = 2000;
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 100;

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct ListMessagesQuery {
    pub limit: Option<i64>,
    pub before: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ChatMessageRow {
    pub id: String,
    pub citizen_id: String,
    pub citizen_name: String,
    pub content: String,
    pub created_at: chrono::DateTime<Utc>,
}

#[derive(Debug)]
pub enum ChatError {
    Message(String),
    Database(String),
}

impl std::fmt::Display for ChatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatError::Message(m) => write!(f, "{m}"),
            ChatError::Database(m) => write!(f, "database error: {m}"),
        }
    }
}

impl ChatError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    fn to_response(self) -> (StatusCode, Json<ApiResponse>) {
        let (status, message) = match &self {
            ChatError::Message(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ChatError::Database(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        (status, Json(ApiResponse::error(message)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GossipInsertResult {
    Inserted,
    AlreadyExists,
}

pub async fn insert_gossip_message(
    pool: &PgPool,
    message: &ChatMessageRow,
) -> Result<GossipInsertResult, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO chat_messages (id, citizen_id, citizen_name, content, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(&message.id)
    .bind(&message.citizen_id)
    .bind(&message.citizen_name)
    .bind(&message.content)
    .bind(message.created_at)
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        tracing::debug!(message_id = %message.id, "chat gossip message inserted");
        Ok(GossipInsertResult::Inserted)
    } else {
        Ok(GossipInsertResult::AlreadyExists)
    }
}

pub fn validate_message_content(content: &str) -> Result<String, ChatError> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err(ChatError::bad_request("Сообщение не может быть пустым"));
    }
    if trimmed.len() > MAX_MESSAGE_LEN {
        return Err(ChatError::bad_request(format!(
            "Сообщение слишком длинное (макс. {MAX_MESSAGE_LEN} символов)"
        )));
    }
    Ok(trimmed.to_string())
}

fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

async fn load_citizen(pool: &PgPool, citizen_id: &str) -> Result<(String, String, String), ChatError> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT id, name, status FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ChatError::Database(e.to_string()))?;

    row.ok_or_else(|| ChatError::bad_request("Гражданин не найден"))
}

fn ensure_active_status(status: &str) -> Result<(), ChatError> {
    if status != "active" {
        return Err(ChatError::bad_request(format!(
            "Отправка недоступна: статус {status}"
        )));
    }
    Ok(())
}

async fn resolve_auth_citizen_id(auth: &AuthContext, pool: &PgPool) -> Result<String, ChatError> {
    if auth.is_node {
        return Err(ChatError::bad_request(
            "Учётные данные узла не могут участвовать в чате",
        ));
    }
    Ok(auth.resolve_account_id(pool).await)
}

pub async fn send_message(
    state: Arc<AppState>,
    auth: &AuthContext,
    req: SendMessageRequest,
) -> Result<ChatMessageRow, ChatError> {
    let citizen_id = resolve_auth_citizen_id(auth, &state.db).await?;
    let (_, citizen_name, status) = load_citizen(&state.db, &citizen_id).await?;
    ensure_active_status(&status)?;

    let content = validate_message_content(&req.content)?;
    let id = Uuid::new_v4().to_string();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO chat_messages (id, citizen_id, citizen_name, content, created_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(&citizen_id)
    .bind(&citizen_name)
    .bind(&content)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| ChatError::Database(e.to_string()))?;

    tracing::info!(citizen = %citizen_name, message_id = %id, "chat message sent");

    let row = ChatMessageRow {
        id,
        citizen_id,
        citizen_name,
        content,
        created_at: now,
    };

    let state_gossip = state.clone();
    let row_gossip = row.clone();
    tokio::spawn(async move {
        crate::gossip::push_chat_message_to_peers(&state_gossip, &row_gossip).await;
    });

    Ok(row)
}

pub async fn list_messages(
    pool: &PgPool,
    query: ListMessagesQuery,
) -> Result<Vec<ChatMessageRow>, ChatError> {
    let limit = clamp_limit(query.limit);

    let mut rows = if let Some(before_id) = query
        .before
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let cursor: Option<(chrono::DateTime<Utc>, String)> = sqlx::query_as(
            "SELECT created_at, id FROM chat_messages WHERE id = $1",
        )
        .bind(before_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ChatError::Database(e.to_string()))?;

        let Some((before_ts, before_msg_id)) = cursor else {
            return Err(ChatError::bad_request("Сообщение для пагинации не найдено"));
        };

        sqlx::query_as::<_, ChatMessageRow>(
            r#"
            SELECT id, citizen_id, citizen_name, content, created_at
            FROM chat_messages
            WHERE created_at < $1 OR (created_at = $1 AND id < $2)
            ORDER BY created_at DESC, id DESC
            LIMIT $3
            "#,
        )
        .bind(before_ts)
        .bind(before_msg_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| ChatError::Database(e.to_string()))?
    } else {
        sqlx::query_as::<_, ChatMessageRow>(
            r#"
            SELECT id, citizen_id, citizen_name, content, created_at
            FROM chat_messages
            ORDER BY created_at DESC, id DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| ChatError::Database(e.to_string()))?
    };

    rows.reverse();
    Ok(rows)
}

pub async fn list_messages_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListMessagesQuery>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot list chat messages");
    }

    match list_messages(&state.db, query).await {
        Ok(rows) => Json(ApiResponse::success(serde_json::json!({
            "messages": rows,
            "total": rows.len(),
        })))
        .into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn send_message_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<SendMessageRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot send chat messages");
    }

    match send_message(state, &auth, req).await {
        Ok(row) => (StatusCode::CREATED, Json(ApiResponse::success(row))).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn gossip_chat_message_handler(
    State(state): State<Arc<AppState>>,
    Json(message): Json<ChatMessageRow>,
) -> impl IntoResponse {
    match crate::gossip::receive_gossip_chat_message(&state, message).await {
        Ok(resp) => Json(resp).into_response(),
        Err(msg) => response::bad_request(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_message_rejects_empty() {
        assert!(validate_message_content("").is_err());
        assert!(validate_message_content("   ").is_err());
    }

    #[test]
    fn validate_message_trims_and_accepts() {
        assert_eq!(validate_message_content("  hello  ").unwrap(), "hello");
    }

    #[test]
    fn validate_message_rejects_too_long() {
        let long = "x".repeat(MAX_MESSAGE_LEN + 1);
        assert!(validate_message_content(&long).is_err());
    }

    #[test]
    fn validate_message_accepts_max_length() {
        let max = "x".repeat(MAX_MESSAGE_LEN);
        assert!(validate_message_content(&max).is_ok());
    }

    #[test]
    fn clamp_limit_bounds() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(500)), MAX_LIMIT);
    }
}
