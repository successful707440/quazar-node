use std::sync::Arc;

use axum::{
    extract::{Extension, State},
    response::IntoResponse,
    Json,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::AuthContext;
use crate::response::{self, ApiResponse};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateVoteRequest {
    pub title: String,
    pub description: String,
    #[serde(default = "default_duration_secs")]
    pub duration_secs: i64,
}

fn default_duration_secs() -> i64 {
    86400
}

#[derive(Debug, Deserialize)]
pub struct FinalizeVoteRequest {
    pub vote_id: String,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct VoteRecord {
    pub vote_id: String,
    pub title: String,
    pub description: String,
    pub start_time: chrono::DateTime<Utc>,
    pub end_time: chrono::DateTime<Utc>,
    pub status: String,
}

pub async fn create_vote(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateVoteRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_citizens() {
        return response::forbidden("Only Aiya and Guardian can start votes");
    }
    if req.title.trim().is_empty() || req.description.trim().is_empty() {
        return response::bad_request("title and description are required");
    }
    if req.duration_secs < 60 {
        return response::bad_request("duration_secs must be at least 60");
    }

    let vote_id = format!("vote_{}", Uuid::new_v4());
    let start = Utc::now();
    let end = start + Duration::seconds(req.duration_secs);

    match sqlx::query(
        "INSERT INTO votes (vote_id, title, description, start_time, end_time, status)
         VALUES ($1, $2, $3, $4, $5, 'active')",
    )
    .bind(&vote_id)
    .bind(req.title.trim())
    .bind(req.description.trim())
    .bind(start)
    .bind(end)
    .execute(&state.db)
    .await
    {
        Ok(_) => Json(ApiResponse::success(serde_json::json!({
            "vote_id": vote_id,
            "status": "active",
            "start_time": start.to_rfc3339(),
            "end_time": end.to_rfc3339(),
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to create vote: {}", e)),
    }
}

pub async fn list_votes(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot list votes");
    }

    match sqlx::query_as::<_, VoteRecord>(
        "SELECT vote_id, title, description, start_time, end_time, status FROM votes ORDER BY start_time DESC",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(votes) => Json(ApiResponse::success(serde_json::json!({ "votes": votes }))).into_response(),
        Err(e) => response::internal_error(format!("Failed to list votes: {}", e)),
    }
}

pub async fn finalize_vote(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<FinalizeVoteRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_citizens() {
        return response::forbidden("Only Aiya and Guardian can finalize votes");
    }
    if req.vote_id.trim().is_empty() {
        return response::bad_request("vote_id is required");
    }

    let result = sqlx::query(
        "UPDATE votes SET status = 'finalized' WHERE vote_id = $1 AND status IN ('active', 'closed')",
    )
    .bind(req.vote_id.trim())
    .execute(&state.db)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => Json(ApiResponse::success(serde_json::json!({
            "vote_id": req.vote_id,
            "status": "finalized"
        })))
        .into_response(),
        Ok(_) => response::bad_request("Vote not found or already finalized"),
        Err(e) => response::internal_error(format!("Failed to finalize vote: {}", e)),
    }
}

pub async fn vote_is_active(pool: &PgPool, vote_id: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(String, chrono::DateTime<Utc>)> = sqlx::query_as(
        "SELECT status, end_time FROM votes WHERE vote_id = $1",
    )
    .bind(vote_id)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some((status, end_time)) => {
            status == "active" && Utc::now() < end_time
        }
        None => false,
    })
}
