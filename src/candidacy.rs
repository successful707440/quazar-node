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
    build_signed_candidate_appointed_event, build_signed_candidate_approved_event,
    build_signed_candidate_nominated_event, build_signed_candidate_voted_event,
    build_signed_citizen_role_event,
};
use crate::gossip;
use crate::models::Event;
use crate::pending;
use crate::response::{self, ApiResponse};
use crate::types::Role;
use crate::validator::EventValidator;
use crate::AppState;

const APPROVAL_PERCENT: i64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidacyStatus {
    Active,
    Approved,
    Appointed,
    Rejected,
}

impl CandidacyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CandidacyStatus::Active => "Active",
            CandidacyStatus::Approved => "Approved",
            CandidacyStatus::Appointed => "Appointed",
            CandidacyStatus::Rejected => "Rejected",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Active" => Some(CandidacyStatus::Active),
            "Approved" => Some(CandidacyStatus::Approved),
            "Appointed" => Some(CandidacyStatus::Appointed),
            "Rejected" => Some(CandidacyStatus::Rejected),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidacyVoteChoice {
    For,
    Against,
    Abstain,
}

impl CandidacyVoteChoice {
    pub fn as_str(&self) -> &'static str {
        match self {
            CandidacyVoteChoice::For => "For",
            CandidacyVoteChoice::Against => "Against",
            CandidacyVoteChoice::Abstain => "Abstain",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "For" => Some(CandidacyVoteChoice::For),
            "Against" => Some(CandidacyVoteChoice::Against),
            "Abstain" => Some(CandidacyVoteChoice::Abstain),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct NominateRequest {
    pub candidate_id: String,
    pub target_role: String,
}

#[derive(Debug, Deserialize)]
pub struct VoteRequest {
    pub vote: String,
}

#[derive(Debug, Deserialize)]
pub struct ListCandidaciesQuery {
    pub status: Option<String>,
    pub target_role: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct CandidacyRow {
    pub id: String,
    pub citizen_id: String,
    pub citizen_name: String,
    pub target_role: String,
    pub status: String,
    pub votes_for: i32,
    pub votes_against: i32,
    pub votes_abstain: i32,
    pub threshold: i32,
    pub nominator_id: String,
    pub nominator_name: String,
    pub created_at: chrono::DateTime<Utc>,
    pub approved_at: Option<chrono::DateTime<Utc>>,
    pub appointed_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug)]
pub enum CandidacyError {
    Message(String),
    Database(String),
}

impl std::fmt::Display for CandidacyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CandidacyError::Message(m) => write!(f, "{m}"),
            CandidacyError::Database(m) => write!(f, "database error: {m}"),
        }
    }
}

impl CandidacyError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    fn to_response(self) -> (StatusCode, Json<ApiResponse>) {
        let (status, message) = match &self {
            CandidacyError::Message(m) => (StatusCode::BAD_REQUEST, m.clone()),
            CandidacyError::Database(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        };
        (status, Json(ApiResponse::error(message)))
    }
}

/// 5% of all registered citizens, minimum 1 vote required.
pub fn approval_threshold(citizen_count: i64) -> i32 {
    if citizen_count <= 0 {
        return 1;
    }
    let votes = (citizen_count * APPROVAL_PERCENT + 99) / 100;
    votes.max(1) as i32
}

pub fn is_elevated_role(role: &str) -> bool {
    matches!(role, "Guardian" | "Judge" | "Aiya")
}

async fn citizen_count(pool: &PgPool) -> Result<i64, CandidacyError> {
    sqlx::query_scalar("SELECT COUNT(*)::bigint FROM citizens")
        .fetch_one(pool)
        .await
        .map_err(|e| CandidacyError::Database(e.to_string()))
}

async fn load_citizen(pool: &PgPool, citizen_id: &str) -> Result<(String, String, String), CandidacyError> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT id, name, status FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;

    row.ok_or_else(|| CandidacyError::bad_request("Гражданин не найден"))
}

fn ensure_active_status(status: &str, who: &str) -> Result<(), CandidacyError> {
    if status != "active" {
        return Err(CandidacyError::bad_request(format!(
            "{who} не может участвовать: статус {status}"
        )));
    }
    Ok(())
}

async fn ensure_can_participate(pool: &PgPool, citizen_id: &str) -> Result<(), CandidacyError> {
    let (_, _, status) = load_citizen(pool, citizen_id).await?;
    ensure_active_status(&status, "Гражданин")
}

async fn resolve_auth_citizen_id(auth: &AuthContext, pool: &PgPool) -> Result<String, CandidacyError> {
    if auth.is_node {
        return Err(CandidacyError::bad_request(
            "Учётные данные узла не могут участвовать в кандидатурах",
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

fn pending_submit_error(body: Json<ApiResponse>) -> CandidacyError {
    CandidacyError::Message(
        body.error
            .clone()
            .unwrap_or_else(|| "Не удалось добавить событие".to_string()),
    )
}

const CANDIDACY_SELECT: &str = r#"
    SELECT
        c.id,
        c.citizen_id,
        cand.name AS citizen_name,
        c.target_role,
        c.status,
        c.votes_for,
        c.votes_against,
        c.votes_abstain,
        c.threshold,
        c.nominator_id,
        nom.name AS nominator_name,
        c.created_at,
        c.approved_at,
        c.appointed_at
    FROM candidacies c
    JOIN citizens cand ON cand.id = c.citizen_id
    JOIN citizens nom ON nom.id = c.nominator_id
"#;

async fn fetch_candidacy(pool: &PgPool, id: &str) -> Result<CandidacyRow, CandidacyError> {
    let query = format!("{CANDIDACY_SELECT} WHERE c.id = $1");
    sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| CandidacyError::Database(e.to_string()))?
        .ok_or_else(|| CandidacyError::bad_request("Кандидатура не найдена"))
}

async fn active_candidacy_exists(
    pool: &PgPool,
    citizen_id: &str,
    target_role: &str,
) -> Result<bool, CandidacyError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM candidacies
            WHERE citizen_id = $1 AND target_role = $2 AND status IN ('Active', 'Approved')
        )",
    )
    .bind(citizen_id)
    .bind(target_role)
    .fetch_one(pool)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;
    Ok(exists)
}

pub async fn nominate_candidate(
    state: Arc<AppState>,
    auth: &AuthContext,
    req: NominateRequest,
) -> Result<CandidacyRow, CandidacyError> {
    let nominator_id = resolve_auth_citizen_id(auth, &state.db).await?;
    ensure_can_participate(&state.db, &nominator_id).await?;

    let target_role = req.target_role.trim().to_string();
    if !is_elevated_role(&target_role) {
        return Err(CandidacyError::bad_request(
            "target_role must be Guardian, Judge, or Aiya",
        ));
    }
    if Role::from_str(&target_role).is_none() {
        return Err(CandidacyError::bad_request("Invalid target_role"));
    }

    let candidate_id = req.candidate_id.trim().to_string();
    let (cid, candidate_name, candidate_status) = load_citizen(&state.db, &candidate_id).await?;
    ensure_active_status(&candidate_status, "Кандидат")?;

    if active_candidacy_exists(&state.db, &cid, &target_role).await? {
        return Err(CandidacyError::bad_request(
            "У гражданина уже есть активная кандидатура на эту роль",
        ));
    }

    let threshold = approval_threshold(citizen_count(&state.db).await?);
    let candidacy_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO candidacies (id, citizen_id, target_role, status, votes_for, votes_against, votes_abstain, threshold, nominator_id, created_at)
         VALUES ($1, $2, $3, 'Active', 0, 0, 0, $4, $5, $6)",
    )
    .bind(&candidacy_id)
    .bind(&cid)
    .bind(&target_role)
    .bind(threshold)
    .bind(&nominator_id)
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;

    let (_, nominator_name, _) = load_citizen(&state.db, &nominator_id).await?;
    let event_id = format!("candidate_nominated_{}", candidacy_id);
    let event = build_signed_candidate_nominated_event(
        &event_id,
        &candidacy_id,
        &cid,
        &candidate_name,
        &target_role,
        &nominator_id,
        &nominator_name,
        threshold,
        &auth.citizen_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_candidacy(&state.db, &candidacy_id).await
}

async fn refresh_vote_counts(pool: &PgPool, candidacy_id: &str) -> Result<(), CandidacyError> {
    sqlx::query(
        r#"
        UPDATE candidacies SET
            votes_for = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'For'),
            votes_against = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'Against'),
            votes_abstain = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'Abstain')
        WHERE id = $1
        "#,
    )
    .bind(candidacy_id)
    .execute(pool)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;
    Ok(())
}

pub async fn approve_candidate(
    state: Arc<AppState>,
    auth_name: &str,
    candidacy_id: &str,
) -> Result<CandidacyRow, CandidacyError> {
    let row = fetch_candidacy(&state.db, candidacy_id).await?;
    if row.status != CandidacyStatus::Active.as_str() {
        return Ok(row);
    }

    let now = Utc::now();
    sqlx::query(
        "UPDATE candidacies SET status = 'Approved', approved_at = $1 WHERE id = $2 AND status = 'Active'",
    )
    .bind(now)
    .bind(candidacy_id)
    .execute(&state.db)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;

    let event_id = format!("candidate_approved_{}", candidacy_id);
    let event = build_signed_candidate_approved_event(
        &event_id,
        candidacy_id,
        &row.citizen_id,
        &row.citizen_name,
        &row.target_role,
        auth_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_candidacy(&state.db, candidacy_id).await
}

pub async fn vote_for_candidate(
    state: Arc<AppState>,
    auth: &AuthContext,
    candidacy_id: &str,
    req: VoteRequest,
) -> Result<CandidacyRow, CandidacyError> {
    let voter_id = resolve_auth_citizen_id(auth, &state.db).await?;
    ensure_can_participate(&state.db, &voter_id).await?;

    let choice = CandidacyVoteChoice::from_str(req.vote.trim())
        .ok_or_else(|| CandidacyError::bad_request("vote must be For, Against, or Abstain"))?;

    let candidacy = fetch_candidacy(&state.db, candidacy_id).await?;
    if candidacy.status != CandidacyStatus::Active.as_str() {
        return Err(CandidacyError::bad_request(
            "Голосование доступно только для активных кандидатур",
        ));
    }

    let vote_id = Uuid::new_v4().to_string();
    let now = Utc::now();

    let inserted = sqlx::query(
        "INSERT INTO candidacy_votes (id, candidacy_id, citizen_id, vote, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (candidacy_id, citizen_id) DO NOTHING",
    )
    .bind(&vote_id)
    .bind(candidacy_id)
    .bind(&voter_id)
    .bind(choice.as_str())
    .bind(now)
    .execute(&state.db)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;

    if inserted.rows_affected() == 0 {
        return Err(CandidacyError::bad_request("Вы уже голосовали за эту кандидатуру"));
    }

    refresh_vote_counts(&state.db, candidacy_id).await?;

    let (_, voter_name, _) = load_citizen(&state.db, &voter_id).await?;
    let event_id = format!("candidate_voted_{}_{}", candidacy_id, voter_id);
    let event = build_signed_candidate_voted_event(
        &event_id,
        candidacy_id,
        &voter_id,
        &voter_name,
        choice.as_str(),
        &auth.citizen_name,
        now.timestamp(),
    );

    submit_pending_event(state.clone(), event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    let updated = fetch_candidacy(&state.db, candidacy_id).await?;
    if updated.votes_for >= updated.threshold {
        return approve_candidate(state, "system", candidacy_id).await;
    }

    Ok(updated)
}

pub async fn appoint_candidate(
    state: Arc<AppState>,
    auth: &AuthContext,
    candidacy_id: &str,
) -> Result<CandidacyRow, CandidacyError> {
    if !auth.can_change_citizen_role() {
        return Err(CandidacyError::bad_request(
            "Только Айя может назначать кандидатов на роль",
        ));
    }

    let candidacy = fetch_candidacy(&state.db, candidacy_id).await?;
    if candidacy.status != CandidacyStatus::Approved.as_str() {
        return Err(CandidacyError::bad_request(
            "Назначение доступно только для утверждённых кандидатур",
        ));
    }

    let now = Utc::now();
    sqlx::query(
        "UPDATE candidacies SET status = 'Appointed', appointed_at = $1 WHERE id = $2",
    )
    .bind(now)
    .bind(candidacy_id)
    .execute(&state.db)
    .await
    .map_err(|e| CandidacyError::Database(e.to_string()))?;

    let appointed_event_id = format!("candidate_appointed_{}", candidacy_id);
    let appointed_event = build_signed_candidate_appointed_event(
        &appointed_event_id,
        candidacy_id,
        &candidacy.citizen_id,
        &candidacy.citizen_name,
        &candidacy.target_role,
        &auth.citizen_name,
        now.timestamp(),
    );
    submit_pending_event(state.clone(), appointed_event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    let role_event_id = format!(
        "citizen_role_{}_{}",
        candidacy.citizen_id,
        now.timestamp()
    );
    let role_event = build_signed_citizen_role_event(
        &role_event_id,
        &candidacy.citizen_id,
        &candidacy.citizen_name,
        &candidacy.target_role,
        &auth.citizen_name,
        now.timestamp(),
    );
    submit_pending_event(state.clone(), role_event)
        .await
        .map_err(|(_, body)| pending_submit_error(body))?;

    fetch_candidacy(&state.db, candidacy_id).await
}

pub async fn get_candidacy(pool: &PgPool, id: &str) -> Result<CandidacyRow, CandidacyError> {
    fetch_candidacy(pool, id).await
}

pub async fn list_candidacies(
    pool: &PgPool,
    query: ListCandidaciesQuery,
) -> Result<Vec<CandidacyRow>, CandidacyError> {
    let mut sql = format!("{CANDIDACY_SELECT} WHERE 1=1");
    let mut binds: Vec<String> = Vec::new();

    if let Some(status) = query.status.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if CandidacyStatus::from_str(status).is_none() {
            return Err(CandidacyError::bad_request("Invalid candidacy status filter"));
        }
        binds.push(status.to_string());
        sql.push_str(&format!(" AND c.status = ${}", binds.len()));
    }
    if let Some(role) = query
        .target_role
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        binds.push(role.to_string());
        sql.push_str(&format!(" AND c.target_role = ${}", binds.len()));
    }
    sql.push_str(" ORDER BY c.created_at DESC");

    let mut q = sqlx::query_as::<_, CandidacyRow>(&sql);
    for b in &binds {
        q = q.bind(b);
    }

    q.fetch_all(pool)
        .await
        .map_err(|e| CandidacyError::Database(e.to_string()))
}

// --- HTTP handlers ---

pub async fn nominate_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<NominateRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot nominate candidates");
    }

    match nominate_candidate(state, &auth, req).await {
        Ok(row) => (
            StatusCode::ACCEPTED,
            Json(ApiResponse::success(row)),
        )
            .into_response(),
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

    match vote_for_candidate(state, &auth, &id, req).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn appoint_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match appoint_candidate(state, &auth, &id).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn get_candidacy_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match get_candidacy(&state.db, &id).await {
        Ok(row) => Json(ApiResponse::success(row)).into_response(),
        Err(e) => e.to_response().into_response(),
    }
}

pub async fn list_candidacies_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListCandidaciesQuery>,
) -> impl IntoResponse {
    match list_candidacies(&state.db, query).await {
        Ok(rows) => Json(ApiResponse::success(serde_json::json!({
            "candidacies": rows,
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
        assert_eq!(approval_threshold(1), 1);
        assert_eq!(approval_threshold(20), 1);
        assert_eq!(approval_threshold(21), 2);
        assert_eq!(approval_threshold(100), 5);
        assert_eq!(approval_threshold(101), 6);
    }

    #[test]
    fn elevated_roles_only() {
        assert!(is_elevated_role("Guardian"));
        assert!(is_elevated_role("Judge"));
        assert!(is_elevated_role("Aiya"));
        assert!(!is_elevated_role("Citizen"));
    }

    #[test]
    fn vote_choice_parsing() {
        assert_eq!(CandidacyVoteChoice::from_str("For"), Some(CandidacyVoteChoice::For));
        assert_eq!(
            CandidacyVoteChoice::from_str("Against"),
            Some(CandidacyVoteChoice::Against)
        );
        assert!(CandidacyVoteChoice::from_str("Yes").is_none());
    }
}
