use std::sync::Arc;

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

pub use crate::types::Role;

use crate::auth::{registration_signature, AuthContext};
use crate::block_producer;
use crate::blockchain::{
    build_citizen_added_event, build_signed_citizen_role_event, build_signed_citizen_status_event,
    build_signed_passport_issued_event, build_signed_passport_revoked_event, compute_event_hash,
};
use crate::models::Event;
use crate::pending;
use crate::response::ApiResponse;
use crate::validator::EventValidator;
use crate::AppState;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum CitizenStatus {
    Pending,
    Active,
    Suspended,
    Revoked,
}

impl CitizenStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CitizenStatus::Pending => "pending",
            CitizenStatus::Active => "active",
            CitizenStatus::Suspended => "suspended",
            CitizenStatus::Revoked => "revoked",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "pending" => Some(CitizenStatus::Pending),
            "active" => Some(CitizenStatus::Active),
            "suspended" => Some(CitizenStatus::Suspended),
            "revoked" => Some(CitizenStatus::Revoked),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Citizen {
    pub id: String,
    pub name: String,
    pub public_key: String,
    pub status: CitizenStatus,
    pub role: Role,
    pub created_at: i64,
    pub passport_issued: bool,
    pub passport_expires: Option<i64>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct CitizenRow {
    id: String,
    name: String,
    public_key: String,
    status: String,
    role: String,
    created_at: i64,
    passport_issued: bool,
    passport_expires: Option<i64>,
}

impl TryFrom<CitizenRow> for Citizen {
    type Error = CitizenError;

    fn try_from(row: CitizenRow) -> Result<Self, Self::Error> {
        Ok(Citizen {
            id: row.id,
            name: row.name,
            public_key: row.public_key,
            status: CitizenStatus::from_str(&row.status).ok_or(CitizenError::InvalidStatus)?,
            role: Role::from_str(&row.role).ok_or(CitizenError::InvalidRole)?,
            created_at: row.created_at,
            passport_issued: row.passport_issued,
            passport_expires: row.passport_expires,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct RegisterCitizenRequest {
    pub name: String,
    pub public_key: String,
    pub role: Option<String>,
    pub birth_place: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IssuePassportRequest {
    pub expires_in_days: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStatusRequest {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRoleRequest {
    pub role: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
}

#[derive(Debug, Serialize)]
pub struct CitizenResponse {
    pub id: String,
    pub name: String,
    pub public_key: String,
    pub status: String,
    pub role: String,
    pub created_at: i64,
    pub passport_issued: bool,
    pub passport_expires: Option<i64>,
}

impl From<Citizen> for CitizenResponse {
    fn from(c: Citizen) -> Self {
        CitizenResponse {
            id: c.id,
            name: c.name,
            public_key: c.public_key,
            status: c.status.as_str().to_string(),
            role: c.role.as_str().to_string(),
            created_at: c.created_at,
            passport_issued: c.passport_issued,
            passport_expires: c.passport_expires,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RegisterCitizenResponse {
    pub id: String,
    pub name: String,
    pub public_key: String,
    pub role: String,
    pub registration_status: String,
    pub event_id: String,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct PendingCitizenEventResponse {
    pub citizen_id: String,
    pub event_id: String,
    pub event_type: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct PendingPassportEventResponse {
    pub citizen_id: String,
    pub passport_id: String,
    pub event_id: String,
    pub event_type: String,
    pub expires_at: Option<i64>,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct CitizensListResponse {
    pub citizens: Vec<CitizenResponse>,
    pub total: usize,
}

#[derive(Debug)]
pub enum CitizenError {
    CitizenAlreadyExists,
    CitizenNotFound,
    InvalidRole,
    InvalidStatus,
    PassportAlreadyIssued,
    PassportNotFound,
    InsufficientPermissions,
    GovernanceRoleRequiresCandidacy,
    InvalidCitizenName(String),
    InvalidPublicKey,
    DatabaseError(String),
}

impl CitizenError {
    pub fn to_response(&self) -> (StatusCode, Json<ApiResponse>) {
        let (status, message) = match self {
            CitizenError::CitizenAlreadyExists => {
                (StatusCode::CONFLICT, "Гражданин с таким именем уже существует".to_string())
            }
            CitizenError::CitizenNotFound => {
                (StatusCode::NOT_FOUND, "Гражданин не найден".to_string())
            }
            CitizenError::InvalidRole => {
                (StatusCode::BAD_REQUEST, "Некорректная роль".to_string())
            }
            CitizenError::InvalidStatus => {
                (StatusCode::BAD_REQUEST, "Некорректный статус".to_string())
            }
            CitizenError::PassportAlreadyIssued => {
                (StatusCode::CONFLICT, "Паспорт уже выдан".to_string())
            }
            CitizenError::PassportNotFound => {
                (StatusCode::NOT_FOUND, "Паспорт не найден".to_string())
            }
            CitizenError::InsufficientPermissions => {
                (StatusCode::FORBIDDEN, "Недостаточно прав".to_string())
            }
            CitizenError::GovernanceRoleRequiresCandidacy => (
                StatusCode::BAD_REQUEST,
                "Роли Guardian, Judge и Aiya назначаются только через процесс кандидатуры (nominate → vote → appoint)".to_string(),
            ),
            CitizenError::InvalidCitizenName(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            CitizenError::InvalidPublicKey => (
                StatusCode::BAD_REQUEST,
                "public_key должен быть 32 байта в hex (64 символа, допускается префикс 0x)".to_string(),
            ),
            CitizenError::DatabaseError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Ошибка базы данных: {}", msg),
            ),
        };
        (status, Json(ApiResponse::error(message)))
    }
}

fn reject_node(auth: &AuthContext) -> Option<(StatusCode, Json<ApiResponse>)> {
    if auth.is_node {
        Some(CitizenError::InsufficientPermissions.to_response())
    } else {
        None
    }
}

async fn get_citizen_by_name(pool: &PgPool, name: &str) -> Result<Option<Citizen>, CitizenError> {
    let row = sqlx::query_as::<_, CitizenRow>(
        "SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires
         FROM citizens WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| CitizenError::DatabaseError(e.to_string()))?;

    row.map(Citizen::try_from).transpose()
}

async fn get_citizen_by_id(pool: &PgPool, id: &str) -> Result<Option<Citizen>, CitizenError> {
    let row = sqlx::query_as::<_, CitizenRow>(
        "SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires
         FROM citizens WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| CitizenError::DatabaseError(e.to_string()))?;

    row.map(Citizen::try_from).transpose()
}

async fn citizen_name_taken(pool: &PgPool, name: &str) -> Result<bool, CitizenError> {
    if get_citizen_by_name(pool, name).await?.is_some() {
        return Ok(true);
    }
    let in_pending: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pending_events
            WHERE event_data::jsonb->>'event_type' = 'CitizenAdded'
              AND event_data::jsonb->'data'->>'citizen_name' = $1
        )
        "#,
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| CitizenError::DatabaseError(e.to_string()))?;
    Ok(in_pending)
}

async fn pending_event_for_citizen(
    pool: &PgPool,
    event_type: &str,
    citizen_id: &str,
) -> Result<bool, CitizenError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pending_events
            WHERE event_data::jsonb->>'event_type' = $1
              AND event_data::jsonb->'data'->>'citizen_id' = $2
        )
        "#,
    )
    .bind(event_type)
    .bind(citizen_id)
    .fetch_one(pool)
    .await
    .map_err(|e| CitizenError::DatabaseError(e.to_string()))?;
    Ok(exists)
}

async fn get_valid_passport_id(pool: &PgPool, citizen_id: &str) -> Result<Option<String>, CitizenError> {
    sqlx::query_scalar(
        "SELECT id FROM passports WHERE citizen_id = $1 AND is_valid = TRUE ORDER BY issued_at DESC LIMIT 1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| CitizenError::DatabaseError(e.to_string()))
}

async fn submit_pending_event(
    state: Arc<AppState>,
    event: Event,
) -> Result<(), (StatusCode, Json<ApiResponse>)> {
    if let Err(e) = EventValidator::validate_event(&event, &state.db).await {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(e.message())),
        ));
    }

    match pending::insert(&state.db, &event).await {
        Ok(pending::PendingInsertResult::Inserted) => {}
        Ok(pending::PendingInsertResult::AlreadyExists) => {
            return Ok(());
        }
        Err(e) => {
            return Err(CitizenError::DatabaseError(format!("Не удалось добавить событие: {}", e))
                .to_response());
        }
    }

    let state_gossip = state.clone();
    let event_gossip = event.clone();
    tokio::spawn(async move {
        crate::gossip::push_pending_to_peers(&state_gossip, &event_gossip).await;
    });

    let state_clone = state.clone();
    tokio::spawn(async move {
        block_producer::try_create_block(state_clone).await;
    });

    Ok(())
}

pub async fn register_citizen(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<RegisterCitizenRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return CitizenError::InsufficientPermissions.to_response();
    }

    if req.name.is_empty() || !req.name.chars().all(|c| c.is_ascii_alphabetic()) {
        return CitizenError::InvalidCitizenName(
            "Имя должно содержать только латинские буквы".to_string(),
        )
        .to_response();
    }

    let public_key = match crate::crypto::validate_public_key_hex(&req.public_key) {
        Ok(key) => key,
        Err(_) => return CitizenError::InvalidPublicKey.to_response(),
    };

    if citizen_name_taken(&state.db, &req.name).await.unwrap_or(false) {
        return CitizenError::CitizenAlreadyExists.to_response();
    }

    // Default Citizen on registration is membership, not governance role assignment.
    let role = match req.role {
        Some(r) => match Role::from_str(&r) {
            Some(role) => role,
            None => return CitizenError::InvalidRole.to_response(),
        },
        None => Role::Citizen,
    };

    if role.is_governance() {
        return CitizenError::GovernanceRoleRequiresCandidacy.to_response();
    }

    let citizen_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let birth_place = req
        .birth_place
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "Quazar".to_string());
    let event_id = format!("citizen_add_{}", citizen_id);

    let mut event = build_citizen_added_event(
        &event_id,
        &citizen_id,
        &req.name,
        &public_key,
        &birth_place,
        role.as_str(),
        &auth.citizen_name,
        now,
    );
    event.hash = Some(compute_event_hash(&event));
    event.signatures = vec![registration_signature(
        &citizen_id,
        &public_key,
        event.hash.as_deref().unwrap_or(""),
    )];

    if let Err(resp) = submit_pending_event(state.clone(), event).await {
        return resp;
    }

    (
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(RegisterCitizenResponse {
            id: citizen_id,
            name: req.name,
            public_key,
            role: role.as_str().to_string(),
            registration_status: "pending".to_string(),
            event_id,
            created_at: now,
        })),
    )
}

pub async fn list_citizens(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
) -> impl IntoResponse {
    if let Some(resp) = reject_node(&auth) {
        return resp;
    }

    let rows = match sqlx::query_as::<_, CitizenRow>(
        "SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires
         FROM citizens ORDER BY created_at DESC",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let mut citizens = Vec::new();
    for row in rows {
        match Citizen::try_from(row) {
            Ok(c) => citizens.push(CitizenResponse::from(c)),
            Err(e) => return e.to_response(),
        }
    }

    let response = CitizensListResponse {
        total: citizens.len(),
        citizens,
    };
    (StatusCode::OK, Json(ApiResponse::success(response)))
}

pub async fn get_citizen(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = reject_node(&auth) {
        return resp;
    }

    let citizen = match get_citizen_by_id(&state.db, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if !auth.is_master && !auth.authorize_citizen_ref(&state.db, &citizen.id).await {
        return CitizenError::InsufficientPermissions.to_response();
    }

    (StatusCode::OK, Json(ApiResponse::success(CitizenResponse::from(citizen))))
}

pub async fn update_status(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateStatusRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_citizens() {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let status = match CitizenStatus::from_str(&req.status) {
        Some(s) => s,
        None => return CitizenError::InvalidStatus.to_response(),
    };

    let citizen = match get_citizen_by_id(&state.db, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if citizen.status == status {
        return (
            StatusCode::OK,
            Json(ApiResponse::success(CitizenResponse::from(citizen))),
        );
    }

    let now = chrono::Utc::now().timestamp();
    let event_id = format!("citizen_status_{}_{}", citizen.id, now);
    let event = build_signed_citizen_status_event(
        &event_id,
        &citizen.id,
        &citizen.name,
        status.as_str(),
        &auth.citizen_name,
        now,
    );

    if let Err(resp) = submit_pending_event(state, event.clone()).await {
        return resp;
    }

    (
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(PendingCitizenEventResponse {
            citizen_id: citizen.id,
            event_id: event.event_id,
            event_type: event.event_type,
            status: "pending".to_string(),
        })),
    )
}

pub async fn update_role(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateRoleRequest>,
) -> impl IntoResponse {
    if !auth.can_change_citizen_role() {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let new_role = match Role::from_str(&req.role) {
        Some(role) => role,
        None => return CitizenError::InvalidRole.to_response(),
    };

    let mut citizen = match get_citizen_by_id(&state.db, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if citizen.role == new_role {
        return (
            StatusCode::OK,
            Json(ApiResponse::success(CitizenResponse::from(citizen))),
        );
    }

    if new_role.is_governance() {
        return CitizenError::GovernanceRoleRequiresCandidacy.to_response();
    }

    let now = chrono::Utc::now().timestamp();
    let event_id = format!("citizen_role_{}_{}", citizen.id, now);
    let event = build_signed_citizen_role_event(
        &event_id,
        &citizen.id,
        &citizen.name,
        new_role.as_str(),
        &auth.citizen_name,
        now,
    );

    if let Err(resp) = submit_pending_event(state, event).await {
        return resp;
    }

    citizen.role = new_role;
    (
        StatusCode::OK,
        Json(ApiResponse::success(CitizenResponse::from(citizen))),
    )
}

pub async fn issue_passport(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<IssuePassportRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_citizens() {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let citizen = match get_citizen_by_id(&state.db, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if citizen.passport_issued {
        return CitizenError::PassportAlreadyIssued.to_response();
    }

    if pending_event_for_citizen(&state.db, "PassportIssued", &citizen.id)
        .await
        .unwrap_or(false)
    {
        return CitizenError::PassportAlreadyIssued.to_response();
    }

    let passport_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let expires_in_days = req.expires_in_days.unwrap_or(365);
    let expires_at = now + (expires_in_days * 24 * 60 * 60) as i64;
    let event_id = format!("passport_issue_{}", passport_id);

    let event = build_signed_passport_issued_event(
        &event_id,
        &passport_id,
        &citizen.id,
        &citizen.name,
        now,
        expires_at,
        &auth.citizen_name,
        now,
    );

    if let Err(resp) = submit_pending_event(state, event.clone()).await {
        return resp;
    }

    (
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(PendingPassportEventResponse {
            citizen_id: citizen.id,
            passport_id,
            event_id: event.event_id,
            event_type: event.event_type,
            expires_at: Some(expires_at),
            status: "pending".to_string(),
        })),
    )
}

pub async fn revoke_passport(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !auth.can_manage_citizens() {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let citizen = match get_citizen_by_id(&state.db, &id).await {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if !citizen.passport_issued {
        return CitizenError::PassportNotFound.to_response();
    }

    if pending_event_for_citizen(&state.db, "PassportRevoked", &citizen.id)
        .await
        .unwrap_or(false)
    {
        return (
            StatusCode::CONFLICT,
            Json(ApiResponse::error("Аннулирование паспорта уже ожидает подтверждения")),
        );
    }

    let passport_id = match get_valid_passport_id(&state.db, &citizen.id).await {
        Ok(Some(id)) => id,
        Ok(None) => return CitizenError::PassportNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    let now = chrono::Utc::now().timestamp();
    let event_id = format!("passport_revoke_{}", passport_id);
    let event = build_signed_passport_revoked_event(
        &event_id,
        &passport_id,
        &citizen.id,
        &citizen.name,
        &auth.citizen_name,
        now,
    );

    if let Err(resp) = submit_pending_event(state, event.clone()).await {
        return resp;
    }

    (
        StatusCode::ACCEPTED,
        Json(ApiResponse::success(PendingPassportEventResponse {
            citizen_id: citizen.id,
            passport_id,
            event_id: event.event_id,
            event_type: event.event_type,
            expires_at: None,
            status: "pending".to_string(),
        })),
    )
}

pub async fn search_citizens(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    if let Some(resp) = reject_node(&auth) {
        return resp;
    }

    let search_pattern = format!("%{}%", query.q);
    let rows = match sqlx::query_as::<_, CitizenRow>(
        "SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires
         FROM citizens WHERE name LIKE $1 ORDER BY created_at DESC",
    )
    .bind(search_pattern)
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let mut citizens = Vec::new();
    for row in rows {
        match Citizen::try_from(row) {
            Ok(c) => citizens.push(CitizenResponse::from(c)),
            Err(e) => return e.to_response(),
        }
    }

    let response = CitizensListResponse {
        total: citizens.len(),
        citizens,
    };
    (StatusCode::OK, Json(ApiResponse::success(response)))
}
