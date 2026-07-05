use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use axum_extra::headers::{Authorization, authorization::Bearer};
use axum_extra::TypedHeader;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::AppState;

// ============ DATA STRUCTURES ============

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum CitizenStatus {
    Active,
    Suspended,
    Revoked,
}

impl CitizenStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CitizenStatus::Active => "active",
            CitizenStatus::Suspended => "suspended",
            CitizenStatus::Revoked => "revoked",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "active" => Some(CitizenStatus::Active),
            "suspended" => Some(CitizenStatus::Suspended),
            "revoked" => Some(CitizenStatus::Revoked),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Role {
    Citizen,
    Judge,
    Guardian,
    Aiya,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Citizen => "Citizen",
            Role::Judge => "Judge",
            Role::Guardian => "Guardian",
            Role::Aiya => "Aiya",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Citizen" => Some(Role::Citizen),
            "Judge" => Some(Role::Judge),
            "Guardian" => Some(Role::Guardian),
            "Aiya" => Some(Role::Aiya),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Passport {
    pub id: String,
    pub citizen_id: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub is_valid: bool,
}

// ============ REQUESTS ============

#[derive(Debug, Deserialize)]
pub struct RegisterCitizenRequest {
    pub name: String,
    pub public_key: String,
    pub role: Option<String>,
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
pub struct SearchQuery {
    pub q: String,
}

// ============ RESPONSES ============

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
pub struct CitizensListResponse {
    pub citizens: Vec<CitizenResponse>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct PassportResponse {
    pub id: String,
    pub citizen_id: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub is_valid: bool,
}

impl From<Passport> for PassportResponse {
    fn from(p: Passport) -> Self {
        PassportResponse {
            id: p.id,
            citizen_id: p.citizen_id,
            issued_at: p.issued_at,
            expires_at: p.expires_at,
            is_valid: p.is_valid,
        }
    }
}

// ============ API RESPONSE ============

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub status: String,
    pub data: Option<Value>,
    pub error: Option<String>,
}

impl ApiResponse {
    pub fn success<T: Serialize>(data: T) -> Self {
        ApiResponse {
            status: "success".to_string(),
            data: Some(serde_json::to_value(data).unwrap_or(Value::Null)),
            error: None,
        }
    }

    pub fn error(error: String) -> Self {
        ApiResponse {
            status: "error".to_string(),
            data: None,
            error: Some(error),
        }
    }
}

// ============ ERROR HANDLING ============

#[derive(Debug)]
pub enum CitizenError {
    CitizenAlreadyExists,
    CitizenNotFound,
    InvalidRole,
    InvalidStatus,
    PassportAlreadyIssued,
    PassportNotFound,
    InsufficientPermissions,
    DatabaseError(String),
}

impl From<rusqlite::Error> for CitizenError {
    fn from(err: rusqlite::Error) -> Self {
        CitizenError::DatabaseError(err.to_string())
    }
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
            CitizenError::DatabaseError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Ошибка базы данных: {}", msg))
            }
        };
        (status, Json(ApiResponse::error(message)))
    }
}

// ============ DB FUNCTIONS ============

pub fn init_citizen_tables(conn: &Connection) -> SqliteResult<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS citizens (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            public_key TEXT NOT NULL,
            status TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            passport_issued BOOLEAN DEFAULT FALSE,
            passport_expires INTEGER
        );

        CREATE TABLE IF NOT EXISTS passports (
            id TEXT PRIMARY KEY,
            citizen_id TEXT NOT NULL,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            is_valid BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (citizen_id) REFERENCES citizens(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_citizens_name ON citizens(name);
        CREATE INDEX IF NOT EXISTS idx_citizens_status ON citizens(status);
        CREATE INDEX IF NOT EXISTS idx_passports_citizen_id ON passports(citizen_id);
        "#,
    )?;
    Ok(())
}

pub fn get_citizen_id_from_key(api_key: &str) -> Option<String> {
    if api_key == crate::auth::master_key() {
        return Some(crate::auth::MASTER_NAME.to_string());
    }
    None
}

// ============ HELPER FUNCTIONS ============

fn get_citizen_by_name(conn: &Connection, name: &str) -> Result<Option<Citizen>, CitizenError> {
    let mut stmt = conn
        .prepare("SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires FROM citizens WHERE name = ?")?;

    let mut rows = stmt.query(params![name])?;

    if let Some(row) = rows.next()? {
        let status_str: String = row.get(3)?;
        let role_str: String = row.get(4)?;
        
        let status = CitizenStatus::from_str(&status_str)
            .ok_or(CitizenError::InvalidStatus)?;
        let role = Role::from_str(&role_str)
            .ok_or(CitizenError::InvalidRole)?;

        Ok(Some(Citizen {
            id: row.get(0)?,
            name: row.get(1)?,
            public_key: row.get(2)?,
            status,
            role,
            created_at: row.get(5)?,
            passport_issued: row.get(6)?,
            passport_expires: row.get(7)?,
        }))
    } else {
        Ok(None)
    }
}

fn get_citizen_by_id(conn: &Connection, id: &str) -> Result<Option<Citizen>, CitizenError> {
    let mut stmt = conn
        .prepare("SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires FROM citizens WHERE id = ?")?;

    let mut rows = stmt.query(params![id])?;

    if let Some(row) = rows.next()? {
        let status_str: String = row.get(3)?;
        let role_str: String = row.get(4)?;
        
        let status = CitizenStatus::from_str(&status_str)
            .ok_or(CitizenError::InvalidStatus)?;
        let role = Role::from_str(&role_str)
            .ok_or(CitizenError::InvalidRole)?;

        Ok(Some(Citizen {
            id: row.get(0)?,
            name: row.get(1)?,
            public_key: row.get(2)?,
            status,
            role,
            created_at: row.get(5)?,
            passport_issued: row.get(6)?,
            passport_expires: row.get(7)?,
        }))
    } else {
        Ok(None)
    }
}

// ============ API HANDLERS ============

pub async fn register_citizen(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Json(req): Json<RegisterCitizenRequest>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let _ = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    let mut conn = state.db.lock().await;
    
    if let Ok(Some(_)) = get_citizen_by_name(&conn, &req.name) {
        return CitizenError::CitizenAlreadyExists.to_response();
    }

    let role = match req.role {
        Some(r) => match Role::from_str(&r) {
            Some(role) => role,
            None => return CitizenError::InvalidRole.to_response(),
        },
        None => Role::Citizen,
    };

    let citizen_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let status = CitizenStatus::Active;

    let result = conn.execute(
        "INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            citizen_id,
            req.name,
            req.public_key,
            status.as_str(),
            role.as_str(),
            now,
            false,
        ],
    );

    match result {
        Ok(_) => {
            if let Ok(Some(citizen)) = get_citizen_by_name(&conn, &req.name) {
                let response = CitizenResponse::from(citizen);
                (StatusCode::CREATED, Json(ApiResponse::success(response)))
            } else {
                CitizenError::DatabaseError("Не удалось получить созданного гражданина".to_string()).to_response()
            }
        }
        Err(e) => CitizenError::DatabaseError(e.to_string()).to_response(),
    }
}

pub async fn list_citizens(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let _ = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    let mut conn = state.db.lock().await;
    
    let mut stmt = match conn.prepare("SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires FROM citizens ORDER BY created_at DESC") {
        Ok(stmt) => stmt,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let rows = match stmt.query_map([], |row| {
        let status_str: String = row.get(3)?;
        let role_str: String = row.get(4)?;
        
        let status = CitizenStatus::from_str(&status_str)
            .ok_or(rusqlite::Error::InvalidQuery)?;
        let role = Role::from_str(&role_str)
            .ok_or(rusqlite::Error::InvalidQuery)?;

        Ok(Citizen {
            id: row.get(0)?,
            name: row.get(1)?,
            public_key: row.get(2)?,
            status,
            role,
            created_at: row.get(5)?,
            passport_issued: row.get(6)?,
            passport_expires: row.get(7)?,
        })
    }) {
        Ok(rows) => rows,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let citizens: Vec<Citizen> = rows
        .filter_map(|r| r.ok())
        .collect();

    let total = citizens.len();
    let responses: Vec<CitizenResponse> = citizens.into_iter().map(Into::into).collect();

    let response = CitizensListResponse {
        citizens: responses,
        total,
    };

    (StatusCode::OK, Json(ApiResponse::success(response)))
}

pub async fn get_citizen(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let requester = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    let mut conn = state.db.lock().await;
    
    let citizen = match get_citizen_by_id(&conn, &id) {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    let is_self = requester == citizen.name || requester == "successful";

    if !is_self {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let response = CitizenResponse::from(citizen);
    (StatusCode::OK, Json(ApiResponse::success(response)))
}

pub async fn update_status(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateStatusRequest>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let requester = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    // Только мастер-ключ может менять статус
    if requester != "successful" {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let mut conn = state.db.lock().await;

    let status = match CitizenStatus::from_str(&req.status) {
        Some(s) => s,
        None => return CitizenError::InvalidStatus.to_response(),
    };

    match get_citizen_by_id(&conn, &id) {
        Ok(Some(_)) => {},
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    let result = conn.execute(
        "UPDATE citizens SET status = ? WHERE id = ?",
        params![status.as_str(), id],
    );

    match result {
        Ok(_) => {
            if let Ok(Some(updated)) = get_citizen_by_id(&conn, &id) {
                let response = CitizenResponse::from(updated);
                (StatusCode::OK, Json(ApiResponse::success(response)))
            } else {
                CitizenError::DatabaseError("Не удалось получить обновленного гражданина".to_string()).to_response()
            }
        }
        Err(e) => CitizenError::DatabaseError(e.to_string()).to_response(),
    }
}

pub async fn issue_passport(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<String>,
    Json(req): Json<IssuePassportRequest>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let requester = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    // Только мастер-ключ может выдавать паспорта
    if requester != "successful" {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let mut conn = state.db.lock().await;

    let citizen = match get_citizen_by_id(&conn, &id) {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if citizen.passport_issued {
        return CitizenError::PassportAlreadyIssued.to_response();
    }

    let passport_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let expires_in_days = req.expires_in_days.unwrap_or(365);
    let expires_at = now + (expires_in_days * 24 * 60 * 60) as i64;

    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.execute(
        "INSERT INTO passports (id, citizen_id, issued_at, expires_at, is_valid) VALUES (?, ?, ?, ?, ?)",
        params![passport_id, citizen.id, now, expires_at, true],
    ) {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.execute(
        "UPDATE citizens SET passport_issued = ?, passport_expires = ? WHERE id = ?",
        params![true, expires_at, citizen.id],
    ) {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.commit() {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let mut stmt = match conn.prepare("SELECT id, citizen_id, issued_at, expires_at, is_valid FROM passports WHERE id = ?") {
        Ok(stmt) => stmt,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let mut rows = match stmt.query(params![passport_id]) {
        Ok(rows) => rows,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    if let Some(row) = match rows.next() {
        Ok(Some(row)) => Some(row),
        Ok(None) => return CitizenError::DatabaseError("Паспорт не найден после создания".to_string()).to_response(),
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    } {
        let passport = Passport {
            id: match row.get(0) {
                Ok(id) => id,
                Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
            },
            citizen_id: match row.get(1) {
                Ok(id) => id,
                Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
            },
            issued_at: match row.get(2) {
                Ok(ts) => ts,
                Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
            },
            expires_at: match row.get(3) {
                Ok(ts) => ts,
                Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
            },
            is_valid: match row.get(4) {
                Ok(valid) => valid,
                Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
            },
        };
        let response = PassportResponse::from(passport);
        (StatusCode::CREATED, Json(ApiResponse::success(response)))
    } else {
        CitizenError::DatabaseError("Не удалось получить созданный паспорт".to_string()).to_response()
    }
}

pub async fn revoke_passport(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let requester = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    // Только мастер-ключ может аннулировать паспорта
    if requester != "successful" {
        return CitizenError::InsufficientPermissions.to_response();
    }

    let mut conn = state.db.lock().await;

    let citizen = match get_citizen_by_id(&conn, &id) {
        Ok(Some(c)) => c,
        Ok(None) => return CitizenError::CitizenNotFound.to_response(),
        Err(e) => return e.to_response(),
    };

    if !citizen.passport_issued {
        return CitizenError::PassportNotFound.to_response();
    }

    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.execute(
        "UPDATE passports SET is_valid = ? WHERE citizen_id = ?",
        params![false, citizen.id],
    ) {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.execute(
        "UPDATE citizens SET passport_issued = ?, passport_expires = ? WHERE id = ?",
        params![false, None::<i64>, citizen.id],
    ) {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    match tx.commit() {
        Ok(_) => {},
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    (StatusCode::OK, Json(ApiResponse::success(json!({"message": "Паспорт аннулирован"}))))
}

pub async fn search_citizens(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let api_key = auth.token();
    let _ = match get_citizen_id_from_key(api_key) {
        Some(id) => id,
        None => return CitizenError::InsufficientPermissions.to_response(),
    };

    let mut conn = state.db.lock().await;
    
    let search_pattern = format!("%{}%", query.q);

    let mut stmt = match conn.prepare("SELECT id, name, public_key, status, role, created_at, passport_issued, passport_expires FROM citizens WHERE name LIKE ? ORDER BY created_at DESC") {
        Ok(stmt) => stmt,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let rows = match stmt.query_map(params![search_pattern], |row| {
        let status_str: String = row.get(3)?;
        let role_str: String = row.get(4)?;
        
        let status = CitizenStatus::from_str(&status_str)
            .ok_or(rusqlite::Error::InvalidQuery)?;
        let role = Role::from_str(&role_str)
            .ok_or(rusqlite::Error::InvalidQuery)?;

        Ok(Citizen {
            id: row.get(0)?,
            name: row.get(1)?,
            public_key: row.get(2)?,
            status,
            role,
            created_at: row.get(5)?,
            passport_issued: row.get(6)?,
            passport_expires: row.get(7)?,
        })
    }) {
        Ok(rows) => rows,
        Err(e) => return CitizenError::DatabaseError(e.to_string()).to_response(),
    };

    let citizens: Vec<Citizen> = rows
        .filter_map(|r| r.ok())
        .collect();

    let total = citizens.len();
    let responses: Vec<CitizenResponse> = citizens.into_iter().map(Into::into).collect();

    let response = CitizensListResponse {
        citizens: responses,
        total,
    };

    (StatusCode::OK, Json(ApiResponse::success(response)))
}
