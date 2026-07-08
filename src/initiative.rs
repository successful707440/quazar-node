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
use crate::blockchain::{
    build_signed_law_proposed_event, build_signed_law_vote_result_event,
    build_signed_law_vote_started_event, build_signed_vote_cast_event,
};
use crate::gossip;
use crate::models::Event;
use crate::pending;
use crate::response::{self, ApiResponse};
use crate::validator::EventValidator;
use crate::AppState;

const APPROVAL_PERCENT: i64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitiativeStatus {
    Proposed,
    Passed,
    Rejected,
}

impl InitiativeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InitiativeStatus::Proposed => "Proposed",
            InitiativeStatus::Passed => "Passed",
            InitiativeStatus::Rejected => "Rejected",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Proposed" => Some(InitiativeStatus::Proposed),
            "Passed" => Some(InitiativeStatus::Passed),
            "Rejected" => Some(InitiativeStatus::Rejected),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitiativeVoteChoice {
    For,
    Against,
    Abstain,
}

impl InitiativeVoteChoice {
    pub fn as_str(&self) -> &'static str {
        match self {
            InitiativeVoteChoice::For => "For",
            InitiativeVoteChoice::Against => "Against",
            InitiativeVoteChoice::Abstain => "Abstain",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "For" => Some(InitiativeVoteChoice::For),
            "Against" => Some(InitiativeVoteChoice::Against),
            "Abstain" => Some(InitiativeVoteChoice::Abstain),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ProposeRequest {
    pub title: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct VoteRequest {
    pub vote: String,
}

#[derive(Debug, Deserialize)]
pub struct ListInitiativesQuery {
    pub status: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct InitiativeRow {
    pub id: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub proposer_id: String,
    pub proposer_name: String,
    pub votes_for: i32,
    pub votes_against: i32,
    pub votes_abstain: i32,
    pub threshold: i32,
    pub created_at: chrono::DateTime<Utc>,
    pub passed_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug)]
pub enum InitiativeError {
    Message(String),
    Database(String),
}

impl std::fmt::Display for InitiativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitiativeError::Message(m) => write!(f, "{m}"),
            InitiativeError::Database(m) => write!(f, "database error: {m}"),
        }
    }
}

impl InitiativeError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    fn to_response(self) -> (StatusCode, Json<ApiResponse>) {
        let (status, message) = match &self {
            InitiativeError::Message(m) => (StatusCode::BAD_REQUEST, m.clone()),
            InitiativeError::Database(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        (status, Json(ApiResponse::error(message)))
    }
}

pub fn approval_threshold(citizen_count: i64) -> i32 {
    if citizen_count <= 0 {
        return 1;
    }
    let votes = (citizen_count * APPROVAL_PERCENT + 99) / 100;
    votes.max(1) as i32
}

async fn citizen_count(pool: &PgPool) -> Result<i64, InitiativeError> {
    sqlx::query_scalar("SELECT COUNT(*)::bigint FROM citizens")
        .fetch_one(pool)
        .await
        .map_err(|e| InitiativeError::Database(e.to_string()))
}

async fn load_citizen(pool: &PgPool, citizen_id: &str) -> Result<(String, String, String), InitiativeError> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT id, name, status FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| InitiativeError::Database(e.to_string()))?;

    row.ok_or_else(|| InitiativeError::bad_request("Гражданин не найден"))
}

fn ensure_active_status(status: &str, who: &str) -> Result<(), InitiativeError> {
    if status != "active" {
        return Err(InitiativeError::bad_request(format!(
            "{who} не может участвовать: статус {status}"
        )));
    }
    Ok(())
}

async fn ensure_can_participate(pool: &PgPool, citizen_id: &str) -> Result<(), InitiativeError> {
    let (_, _, status) = load_citizen(pool, citizen_id).await?;
    ensure_active_status(&status, "Гражданин")
}

async fn resolve_auth_citizen_id(auth: &AuthContext, pool: &PgPool) -> Result<String, InitiativeError> {
    if auth.is_node {
        return Err(InitiativeError::bad_request(
            "Учётные данные узла не могут участвовать в инициативах",
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

fn pending_submit_error(body: Json<ApiResponse>) -> InitiativeError {
    InitiativeError::Message(
        body.error
            .clone()
            .unwrap_or_else(|| "Не удалось добавить событие".to_string()),
    )
}

const INITIATIVE_SELECT: &str = r#"
    SELECT
        i.id,
        i.title,
        i.description,
        i.status,
        i.proposer_id,
        p.name AS proposer_name,
        i.votes_for,
        i.votes_against,
        i.votes_abstain,
        i.threshold,
        i.created_at,
        i.passed_at
    FROM initiatives i
    JOIN citizens p ON p.id = i.proposer_id
"#;

async fn fetch_initiative(pool: &PgPool, id: &str) -> Result<InitiativeRow, InitiativeError> {
    let query = format!("{INITIATIVE_SELECT} WHERE i.id = $1");
    sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| InitiativeError::Database(e.to_string()))?
        .ok_or_else(|| InitiativeError::bad_request("Инициатива не найдена"))
}

async fn refresh_vote_counts(pool: &PgPool, initiative_id: &str) -> Result<(), InitiativeError> {
    sqlx::query(
        r#"
        UPDATE initiatives SET
            votes_for = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'For'),
            votes_against = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'Against'),
            votes_abstain = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'Abstain')
        WHERE id = $1
        "#,
    )
    .bind(initiative_id)
    .execute(pool)
    .await
    .map_err(|e| InitiativeError::Database(e.to_string()))?;
    Ok(())
}

pub async fn propose_initiative(
    state: Arc<AppState>,
    auth: &AuthContext,
    req: ProposeRequest,
) -> Result<InitiativeRow, InitiativeError> {
    let proposer_id = resolve_auth_citizen_id(auth, &state.db).await?;
    ensure_can_participate(&state.db, &proposer_id).await?;

    let title = req.title.trim().to_string();
    let description = req.description.trim().to_string();
    if title.is_empty() || description.is_empty() {
        return Err(InitiativeError::bad_request("title и description обязательны"));
    }

    let threshold = approval_threshold(citizen_count(&state.db).await?);
    let initiative_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO initiatives (id, title, description, status, proposer_id, votes_for, votes_against, votes_abstain, threshold, created_at)
         VALUES ($1, $2, $3, 'Proposed', $4, 0, 0, 0, $5, $6)",
    )
    .bind(&initiative_id)
    .bind(&title)
    .bind(&description)
    .bind(&proposer_id)
    .bind(threshold)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| InitiativeError::Database(e.to_string()))?;

    let (_, proposer_name, _) = load_citizen(&state.db, &proposer_id).await?;

    let proposed_event_id = format!("law_proposed_{initiative_id}");
    let proposed_event = build_signed_law_proposed_event(
        &proposed_event_id,
        &initiative_id,
        &title,
        &description,
        &proposer_id,
        &proposer_name,
        &auth.citizen_name,
        now.timestamp(),
    );
    submit_pending_event(state.clone(), proposed_event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    let vote_started_event_id = format!("law_vote_started_{initiative_id}");
    let vote_started_event = build_signed_law_vote_started_event(
        &vote_started_event_id,
        &initiative_id,
        &initiative_id,
        &title,
        &auth.citizen_name,
        now.timestamp(),
    );
    submit_pending_event(state.clone(), vote_started_event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_initiative(&state.db, &initiative_id).await
}

async fn mark_initiative_passed(
    state: Arc<AppState>,
    auth_name: &str,
    initiative_id: &str,
) -> Result<InitiativeRow, InitiativeError> {
    let row = fetch_initiative(&state.db, initiative_id).await?;
    if row.status != InitiativeStatus::Proposed.as_str() {
        return Ok(row);
    }

    let now = Utc::now();
    sqlx::query(
        "UPDATE initiatives SET status = 'Passed', passed_at = $1 WHERE id = $2 AND status = 'Proposed'",
    )
    .bind(now)
    .bind(initiative_id)
    .execute(&state.db)
    .await
    .map_err(|e| InitiativeError::Database(e.to_string()))?;

    let event_id = format!("law_vote_result_{initiative_id}");
    let event = build_signed_law_vote_result_event(
        &event_id,
        initiative_id,
        initiative_id,
        "Passed",
        row.votes_for,
        row.votes_against,
        auth_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_initiative(&state.db, initiative_id).await
}

pub async fn vote_on_initiative(
    state: Arc<AppState>,
    auth: &AuthContext,
    initiative_id: &str,
    req: VoteRequest,
) -> Result<InitiativeRow, InitiativeError> {
    let voter_id = resolve_auth_citizen_id(auth, &state.db).await?;
    ensure_can_participate(&state.db, &voter_id).await?;

    let choice = InitiativeVoteChoice::from_str(req.vote.trim())
        .ok_or_else(|| InitiativeError::bad_request("vote must be For, Against, or Abstain"))?;

    let initiative = fetch_initiative(&state.db, initiative_id).await?;
    if initiative.status != InitiativeStatus::Proposed.as_str() {
        return Err(InitiativeError::bad_request(
            "Голосование доступно только для активных инициатив",
        ));
    }

    let vote_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    let inserted = sqlx::query(
        "INSERT INTO initiative_votes (id, initiative_id, citizen_id, vote, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (initiative_id, citizen_id) DO NOTHING",
    )
    .bind(&vote_id)
    .bind(initiative_id)
    .bind(&voter_id)
    .bind(choice.as_str())
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| InitiativeError::Database(e.to_string()))?;

    if inserted.rows_affected() == 0 {
        return Err(InitiativeError::bad_request("Вы уже голосовали по этой инициативе"));
    }

    refresh_vote_counts(&state.db, initiative_id).await?;

    let (_, voter_name, _) = load_citizen(&state.db, &voter_id).await?;
    let event_id = format!("initiative_vote_{initiative_id}_{voter_id}");
    let event = build_signed_vote_cast_event(
        &event_id,
        initiative_id,
        &voter_id,
        &voter_name,
        choice.as_str(),
        &auth.citizen_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    let updated = fetch_initiative(&state.db, initiative_id).await?;
    if updated.votes_for >= updated.threshold {
        return mark_initiative_passed(state, "system", initiative_id).await;
    }

    Ok(updated)
}

pub async fn get_initiative(pool: &PgPool, id: &str) -> Result<InitiativeRow, InitiativeError> {
    fetch_initiative(pool, id).await
}

pub async fn list_initiatives(
    pool: &PgPool,
    query: ListInitiativesQuery,
) -> Result<Vec<InitiativeRow>, InitiativeError> {
    let mut sql = format!("{INITIATIVE_SELECT} WHERE 1=1");
    let mut binds: Vec<String> = Vec::new();

    if let Some(status) = query.status.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if InitiativeStatus::from_str(status).is_none() {
            return Err(InitiativeError::bad_request("Invalid initiative status filter"));
        }
        binds.push(status.to_string());
        sql.push_str(&format!(" AND i.status = ${}", binds.len()));
    }
    sql.push_str(" ORDER BY i.created_at DESC");

    let mut q = sqlx::query_as::<_, InitiativeRow>(&sql);
    for b in &binds {
        q = q.bind(b);
    }

    q.fetch_all(pool)
        .await
        .map_err(|e| InitiativeError::Database(e.to_string()))
}

// --- HTTP handlers ---

pub async fn propose_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProposeRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot propose initiatives");
    }

    match propose_initiative(state, &auth, req).await {
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

    match vote_on_initiative(state, &auth, &id, req).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn get_initiative_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match get_initiative(&state.db, &id).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn list_initiatives_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListInitiativesQuery>,
) -> impl IntoResponse {
    match list_initiatives(&state.db, query).await {
        Ok(rows) => Json(ApiResponse::success(serde_json::json!({
            "initiatives": rows,
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
    fn approval_threshold_computes_five_percent_ceil() {
        assert_eq!(approval_threshold(0), 1);
        assert_eq!(approval_threshold(100), 5);
    }

    #[test]
    fn vote_choice_parsing() {
        assert_eq!(InitiativeVoteChoice::from_str("For"), Some(InitiativeVoteChoice::For));
        assert!(InitiativeVoteChoice::from_str("Yes").is_none());
    }
}
