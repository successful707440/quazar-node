use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::AuthContext;
use crate::block_producer;
use crate::blockchain::{build_signed_election_announced_event, build_signed_vote_cast_event};
use crate::gossip;
use crate::models::Event;
use crate::pending;
use crate::response::{self, ApiResponse};
use crate::validator::EventValidator;
use crate::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferendumStatus {
    Active,
    Completed,
    Cancelled,
}

impl ReferendumStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReferendumStatus::Active => "Active",
            ReferendumStatus::Completed => "Completed",
            ReferendumStatus::Cancelled => "Cancelled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Active" => Some(ReferendumStatus::Active),
            "Completed" => Some(ReferendumStatus::Completed),
            "Cancelled" => Some(ReferendumStatus::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferendumVoteChoice {
    For,
    Against,
    Abstain,
}

impl ReferendumVoteChoice {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReferendumVoteChoice::For => "For",
            ReferendumVoteChoice::Against => "Against",
            ReferendumVoteChoice::Abstain => "Abstain",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "For" => Some(ReferendumVoteChoice::For),
            "Against" => Some(ReferendumVoteChoice::Against),
            "Abstain" => Some(ReferendumVoteChoice::Abstain),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AnnounceRequest {
    pub title: String,
    pub description: String,
    pub target_decision: String,
}

#[derive(Debug, Deserialize)]
pub struct VoteRequest {
    pub vote: String,
}

#[derive(Debug, Deserialize)]
pub struct ListReferendumsQuery {
    pub status: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ReferendumRow {
    pub id: String,
    pub title: String,
    pub description: String,
    pub target_decision: String,
    pub status: String,
    pub announcer_id: String,
    pub announcer_name: String,
    pub votes_for: i32,
    pub votes_against: i32,
    pub votes_abstain: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug)]
pub enum ReferendumError {
    Message(String),
    Database(String),
}

impl std::fmt::Display for ReferendumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReferendumError::Message(m) => write!(f, "{m}"),
            ReferendumError::Database(m) => write!(f, "database error: {m}"),
        }
    }
}

impl ReferendumError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    fn to_response(self) -> (StatusCode, Json<ApiResponse>) {
        let (status, message) = match &self {
            ReferendumError::Message(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ReferendumError::Database(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        (status, Json(ApiResponse::error(message)))
    }
}

async fn load_citizen(pool: &PgPool, citizen_id: &str) -> Result<(String, String, String), ReferendumError> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT id, name, status FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ReferendumError::Database(e.to_string()))?;

    row.ok_or_else(|| ReferendumError::bad_request("Гражданин не найден"))
}

fn ensure_active_status(status: &str, who: &str) -> Result<(), ReferendumError> {
    if status != "active" {
        return Err(ReferendumError::bad_request(format!(
            "{who} не может участвовать: статус {status}"
        )));
    }
    Ok(())
}

async fn ensure_can_participate(pool: &PgPool, citizen_id: &str) -> Result<(), ReferendumError> {
    let (_, _, status) = load_citizen(pool, citizen_id).await?;
    ensure_active_status(&status, "Гражданин")
}

async fn resolve_auth_citizen_id(auth: &AuthContext, pool: &PgPool) -> Result<String, ReferendumError> {
    if auth.is_node {
        return Err(ReferendumError::bad_request(
            "Учётные данные узла не могут участвовать в референдумах",
        ));
    }
    Ok(auth.resolve_account_id(pool).await)
}

async fn submit_pending_event(
    state: Arc<AppState>,
    event: Event,
) -> Result<(), (StatusCode, Json<ApiResponse>)> {
    if let Err(e) = EventValidator::validate_event(&event, &state.db).await {
        return Err((StatusCode::BAD_REQUEST, Json(ApiResponse::error(e.message()))));
    }

    match pending::insert(&state.db, &event).await {
        Ok(pending::PendingInsertResult::Inserted) => {}
        Ok(pending::PendingInsertResult::AlreadyExists) => return Ok(()),
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!("Не удалось добавить событие: {}", e))),
            ));
        }
    }

    let state_gossip = state.clone();
    let event_gossip = event.clone();
    tokio::spawn(async move {
        gossip::push_pending_to_peers(&state_gossip, &event_gossip).await;
    });

    let state_clone = state.clone();
    tokio::spawn(async move {
        block_producer::try_create_block(state_clone).await;
    });

    Ok(())
}

fn pending_submit_error(body: Json<ApiResponse>) -> ReferendumError {
    ReferendumError::Message(
        body.error
            .clone()
            .unwrap_or_else(|| "Не удалось добавить событие".to_string()),
    )
}

const REFERENDUM_SELECT: &str = r#"
    SELECT
        r.id,
        r.title,
        r.description,
        r.target_decision,
        r.status,
        r.announcer_id,
        a.name AS announcer_name,
        r.votes_for,
        r.votes_against,
        r.votes_abstain,
        r.created_at,
        r.completed_at
    FROM referendums r
    JOIN citizens a ON a.id = r.announcer_id
"#;

async fn fetch_referendum(pool: &PgPool, id: &str) -> Result<ReferendumRow, ReferendumError> {
    let query = format!("{REFERENDUM_SELECT} WHERE r.id = $1");
    sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ReferendumError::Database(e.to_string()))?
        .ok_or_else(|| ReferendumError::bad_request("Референдум не найден"))
}

async fn refresh_vote_counts(pool: &PgPool, referendum_id: &str) -> Result<(), ReferendumError> {
    sqlx::query(
        r#"
        UPDATE referendums SET
            votes_for = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'For'),
            votes_against = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'Against'),
            votes_abstain = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'Abstain')
        WHERE id = $1
        "#,
    )
    .bind(referendum_id)
    .execute(pool)
    .await
    .map_err(|e| ReferendumError::Database(e.to_string()))?;
    Ok(())
}

pub async fn announce_referendum(
    state: Arc<AppState>,
    auth: &AuthContext,
    req: AnnounceRequest,
) -> Result<ReferendumRow, ReferendumError> {
    if !auth.can_change_citizen_role() {
        return Err(ReferendumError::bad_request(
            "Только Айя может объявлять референдумы",
        ));
    }

    let announcer_id = resolve_auth_citizen_id(auth, &state.db).await?;

    let title = req.title.trim().to_string();
    let description = req.description.trim().to_string();
    let target_decision = req.target_decision.trim().to_string();
    if title.is_empty() || description.is_empty() || target_decision.is_empty() {
        return Err(ReferendumError::bad_request(
            "title, description и target_decision обязательны",
        ));
    }

    let referendum_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO referendums (id, title, description, target_decision, status, announcer_id, votes_for, votes_against, votes_abstain, created_at)
         VALUES ($1, $2, $3, $4, 'Active', $5, 0, 0, 0, $6)",
    )
    .bind(&referendum_id)
    .bind(&title)
    .bind(&description)
    .bind(&target_decision)
    .bind(&announcer_id)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| ReferendumError::Database(e.to_string()))?;

    let event_id = format!("election_announced_{referendum_id}");
    let event = build_signed_election_announced_event(
        &event_id,
        &referendum_id,
        &title,
        &target_decision,
        &announcer_id,
        &auth.citizen_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_referendum(&state.db, &referendum_id).await
}

pub async fn vote_on_referendum(
    state: Arc<AppState>,
    auth: &AuthContext,
    referendum_id: &str,
    req: VoteRequest,
) -> Result<ReferendumRow, ReferendumError> {
    let voter_id = resolve_auth_citizen_id(auth, &state.db).await?;
    ensure_can_participate(&state.db, &voter_id).await?;

    let choice = ReferendumVoteChoice::from_str(req.vote.trim())
        .ok_or_else(|| ReferendumError::bad_request("vote must be For, Against, or Abstain"))?;

    let referendum = fetch_referendum(&state.db, referendum_id).await?;
    if referendum.status != ReferendumStatus::Active.as_str() {
        return Err(ReferendumError::bad_request(
            "Голосование доступно только для активных референдумов",
        ));
    }

    let vote_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    let inserted = sqlx::query(
        "INSERT INTO referendum_votes (id, referendum_id, citizen_id, vote, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (referendum_id, citizen_id) DO NOTHING",
    )
    .bind(&vote_id)
    .bind(referendum_id)
    .bind(&voter_id)
    .bind(choice.as_str())
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| ReferendumError::Database(e.to_string()))?;

    if inserted.rows_affected() == 0 {
        return Err(ReferendumError::bad_request("Вы уже голосовали в этом референдуме"));
    }

    refresh_vote_counts(&state.db, referendum_id).await?;

    let (_, voter_name, _) = load_citizen(&state.db, &voter_id).await?;
    let event_id = format!("referendum_vote_{referendum_id}_{voter_id}");
    let event = build_signed_vote_cast_event(
        &event_id,
        referendum_id,
        &voter_id,
        &voter_name,
        choice.as_str(),
        &auth.citizen_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_referendum(&state.db, referendum_id).await
}

pub async fn get_referendum(pool: &PgPool, id: &str) -> Result<ReferendumRow, ReferendumError> {
    fetch_referendum(pool, id).await
}

pub async fn list_referendums(
    pool: &PgPool,
    query: ListReferendumsQuery,
) -> Result<Vec<ReferendumRow>, ReferendumError> {
    let mut sql = format!("{REFERENDUM_SELECT} WHERE 1=1");
    let mut binds: Vec<String> = Vec::new();

    if let Some(status) = query.status.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if ReferendumStatus::from_str(status).is_none() {
            return Err(ReferendumError::bad_request("Invalid referendum status filter"));
        }
        binds.push(status.to_string());
        sql.push_str(&format!(" AND r.status = ${}", binds.len()));
    }
    sql.push_str(" ORDER BY r.created_at DESC");

    let mut q = sqlx::query_as::<_, ReferendumRow>(&sql);
    for b in &binds {
        q = q.bind(b);
    }

    q.fetch_all(pool)
        .await
        .map_err(|e| ReferendumError::Database(e.to_string()))
}

// --- HTTP handlers ---

pub async fn announce_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<AnnounceRequest>,
) -> impl IntoResponse {
    match announce_referendum(state, &auth, req).await {
        Ok(row) => (StatusCode::ACCEPTED, Json(ApiResponse::success(row))).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn vote_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<VoteRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot vote");
    }

    match vote_on_referendum(state, &auth, &id, req).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn get_referendum_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match get_referendum(&state.db, &id).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn list_referendums_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListReferendumsQuery>,
) -> impl IntoResponse {
    match list_referendums(&state.db, query).await {
        Ok(rows) => Json(ApiResponse::success(serde_json::json!({
            "referendums": rows,
            "total": rows.len(),
        })))
        .into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_choice_parsing() {
        assert_eq!(ReferendumVoteChoice::from_str("Against"), Some(ReferendumVoteChoice::Against));
        assert!(ReferendumVoteChoice::from_str("Maybe").is_none());
    }
}
