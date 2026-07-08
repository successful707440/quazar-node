use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::auth::AuthContext;
use crate::response::ApiResponse;
use crate::AppState;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ServiceCategory {
    pub id: i32,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ServiceItem {
    pub id: i32,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub category_id: Option<i32>,
    pub category_code: Option<String>,
    pub category_name: Option<String>,
    pub base_price: i64,
    pub min_quantity: i64,
    pub max_quantity: i64,
    pub is_active: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateServiceRequest {
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub category_id: Option<i32>,
    pub category_code: Option<String>,
    pub base_price: i64,
    pub min_quantity: Option<i64>,
    pub max_quantity: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateServiceRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub category_id: Option<i32>,
    pub category_code: Option<String>,
    pub base_price: Option<i64>,
    pub min_quantity: Option<i64>,
    pub max_quantity: Option<i64>,
    pub is_active: Option<bool>,
}

#[derive(Debug)]
pub enum SvodError {
    NotFound(String),
    Conflict(String),
    BadRequest(String),
    Forbidden(String),
    Database(String),
}

impl SvodError {
    fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            SvodError::NotFound(_) => StatusCode::NOT_FOUND,
            SvodError::Conflict(_) => StatusCode::CONFLICT,
            SvodError::BadRequest(_) => StatusCode::BAD_REQUEST,
            SvodError::Forbidden(_) => StatusCode::FORBIDDEN,
            SvodError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl std::fmt::Display for SvodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SvodError::NotFound(msg) => write!(f, "{msg}"),
            SvodError::Conflict(msg) => write!(f, "{msg}"),
            SvodError::BadRequest(msg) => write!(f, "{msg}"),
            SvodError::Forbidden(msg) => write!(f, "{msg}"),
            SvodError::Database(msg) => write!(f, "Database error: {msg}"),
        }
    }
}

impl IntoResponse for SvodError {
    fn into_response(self) -> axum::response::Response {
        (self.status(), Json(ApiResponse::error(self.to_string()))).into_response()
    }
}

type SvodResult<T> = Result<T, SvodError>;

const SERVICE_SELECT: &str = "SELECT s.id, s.code, s.name, s.description, s.category_id,
    c.code AS category_code, c.name AS category_name,
    s.base_price, s.min_quantity, s.max_quantity, s.is_active
    FROM service_catalog s
    LEFT JOIN service_categories c ON s.category_id = c.id";

fn normalize_code(code: &str) -> Result<String, SvodError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(SvodError::BadRequest("Service code is required".to_string()));
    }
    if !code
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(SvodError::BadRequest(
            "Service code may contain only letters, digits, underscore and hyphen".to_string(),
        ));
    }
    Ok(code.to_string())
}

async fn resolve_category_id(
    pool: &PgPool,
    category_id: Option<i32>,
    category_code: Option<&str>,
) -> SvodResult<Option<i32>> {
    if let Some(id) = category_id {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM service_categories WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|e| SvodError::Database(e.to_string()))?;
        if !exists {
            return Err(SvodError::BadRequest(format!(
                "Category id {id} not found"
            )));
        }
        return Ok(Some(id));
    }
    if let Some(code) = category_code.map(str::trim).filter(|c| !c.is_empty()) {
        let id: Option<i32> = sqlx::query_scalar(
            "SELECT id FROM service_categories WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(pool)
        .await
        .map_err(|e| SvodError::Database(e.to_string()))?;
        return id
            .map(Some)
            .ok_or_else(|| SvodError::BadRequest(format!("Category code {code} not found")));
    }
    Ok(None)
}

pub async fn get_categories(pool: &PgPool) -> SvodResult<Vec<ServiceCategory>> {
    sqlx::query_as::<_, ServiceCategory>(
        "SELECT id, code, name, description FROM service_categories ORDER BY code",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SvodError::Database(e.to_string()))
}

pub async fn get_catalog(pool: &PgPool, active_only: bool) -> SvodResult<Vec<ServiceItem>> {
    let sql = if active_only {
        format!("{SERVICE_SELECT} WHERE s.is_active = TRUE ORDER BY s.code")
    } else {
        format!("{SERVICE_SELECT} ORDER BY s.code")
    };
    sqlx::query_as::<_, ServiceItem>(&sql)
        .fetch_all(pool)
        .await
        .map_err(|e| SvodError::Database(e.to_string()))
}

pub async fn get_service_by_code(
    pool: &PgPool,
    code: &str,
    active_only: bool,
) -> SvodResult<ServiceItem> {
    let code = normalize_code(code)?;
    let sql = if active_only {
        format!("{SERVICE_SELECT} WHERE s.code = $1 AND s.is_active = TRUE")
    } else {
        format!("{SERVICE_SELECT} WHERE s.code = $1")
    };
    sqlx::query_as::<_, ServiceItem>(&sql)
        .bind(&code)
        .fetch_optional(pool)
        .await
        .map_err(|e| SvodError::Database(e.to_string()))?
        .ok_or_else(|| SvodError::NotFound(format!("Service {code} not found")))
}

pub async fn create_service(pool: &PgPool, req: CreateServiceRequest) -> SvodResult<ServiceItem> {
    let code = normalize_code(&req.code)?;
    if req.name.trim().is_empty() {
        return Err(SvodError::BadRequest("Service name is required".to_string()));
    }
    if req.base_price <= 0 {
        return Err(SvodError::BadRequest(
            "base_price must be greater than 0 (КВАЗИ)".to_string(),
        ));
    }
    let min_quantity = req.min_quantity.unwrap_or(1);
    let max_quantity = req.max_quantity.unwrap_or(100);
    if min_quantity <= 0 || max_quantity <= 0 || min_quantity > max_quantity {
        return Err(SvodError::BadRequest(
            "Invalid min_quantity/max_quantity range".to_string(),
        ));
    }

    let category_id =
        resolve_category_id(pool, req.category_id, req.category_code.as_deref()).await?;

    sqlx::query(
        "INSERT INTO service_catalog (code, name, description, category_id, base_price, min_quantity, max_quantity, is_active, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, NOW())",
    )
    .bind(&code)
    .bind(req.name.trim())
    .bind(req.description.as_deref())
    .bind(category_id)
    .bind(req.base_price)
    .bind(min_quantity)
    .bind(max_quantity)
    .execute(pool)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") {
            SvodError::Conflict(format!("Service code {code} already exists"))
        } else {
            SvodError::Database(e.to_string())
        }
    })?;

    tracing::info!(code = %code, base_price = req.base_price, "svod service created");
    get_service_by_code(pool, &code, false).await
}

pub async fn update_service(
    pool: &PgPool,
    code: &str,
    req: UpdateServiceRequest,
) -> SvodResult<ServiceItem> {
    let code = normalize_code(code)?;
    let existing = get_service_by_code(pool, &code, false).await?;

    let name = req
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&existing.name);
    let description = req.description.as_ref().map(|d| d.as_str()).or(existing.description.as_deref());
    let base_price = req.base_price.unwrap_or(existing.base_price);
    let min_quantity = req.min_quantity.unwrap_or(existing.min_quantity);
    let max_quantity = req.max_quantity.unwrap_or(existing.max_quantity);
    let is_active = req.is_active.unwrap_or(existing.is_active);

    if base_price <= 0 {
        return Err(SvodError::BadRequest(
            "base_price must be greater than 0 (КВАЗИ)".to_string(),
        ));
    }
    if min_quantity <= 0 || max_quantity <= 0 || min_quantity > max_quantity {
        return Err(SvodError::BadRequest(
            "Invalid min_quantity/max_quantity range".to_string(),
        ));
    }

    let category_id = if req.category_id.is_some() || req.category_code.is_some() {
        resolve_category_id(pool, req.category_id, req.category_code.as_deref()).await?
    } else {
        existing.category_id
    };

    sqlx::query(
        "UPDATE service_catalog SET name = $1, description = $2, category_id = $3,
         base_price = $4, min_quantity = $5, max_quantity = $6, is_active = $7, updated_at = NOW()
         WHERE code = $8",
    )
    .bind(name)
    .bind(description)
    .bind(category_id)
    .bind(base_price)
    .bind(min_quantity)
    .bind(max_quantity)
    .bind(is_active)
    .bind(&code)
    .execute(pool)
    .await
    .map_err(|e| SvodError::Database(e.to_string()))?;

    tracing::info!(code = %code, "svod service updated");
    get_service_by_code(pool, &code, false).await
}

pub async fn toggle_service(pool: &PgPool, code: &str, active: bool) -> SvodResult<ServiceItem> {
    let code = normalize_code(code)?;
    let result = sqlx::query(
        "UPDATE service_catalog SET is_active = $1, updated_at = NOW() WHERE code = $2",
    )
    .bind(active)
    .bind(&code)
    .execute(pool)
    .await
    .map_err(|e| SvodError::Database(e.to_string()))?;

    if result.rows_affected() == 0 {
        return Err(SvodError::NotFound(format!("Service {code} not found")));
    }

    tracing::info!(code = %code, active, "svod service toggled");
    get_service_by_code(pool, &code, false).await
}

fn require_aiya(auth: &AuthContext) -> SvodResult<()> {
    if auth.can_manage_svod() {
        Ok(())
    } else if auth.is_node {
        Err(SvodError::Forbidden(
            "Node credentials cannot manage the service catalog".to_string(),
        ))
    } else {
        Err(SvodError::Forbidden(
            "Only Aiya can manage the service catalog".to_string(),
        ))
    }
}

pub async fn get_catalog_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse>, SvodError> {
    let items = get_catalog(&state.db, true).await?;
    Ok(Json(ApiResponse::success(items)))
}

pub async fn get_categories_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ApiResponse>, SvodError> {
    let categories = get_categories(&state.db).await?;
    Ok(Json(ApiResponse::success(categories)))
}

pub async fn get_service_handler(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> Result<Json<ApiResponse>, SvodError> {
    let item = get_service_by_code(&state.db, &code, true).await?;
    Ok(Json(ApiResponse::success(item)))
}

pub async fn create_service_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateServiceRequest>,
) -> Result<Json<ApiResponse>, SvodError> {
    require_aiya(&auth)?;
    let item = create_service(&state.db, req).await?;
    Ok(Json(ApiResponse::success(item)))
}

pub async fn update_service_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
    Json(req): Json<UpdateServiceRequest>,
) -> Result<Json<ApiResponse>, SvodError> {
    require_aiya(&auth)?;
    let item = update_service(&state.db, &code, req).await?;
    Ok(Json(ApiResponse::success(item)))
}

pub async fn disable_service_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> Result<Json<ApiResponse>, SvodError> {
    require_aiya(&auth)?;
    let item = toggle_service(&state.db, &code, false).await?;
    Ok(Json(ApiResponse::success(serde_json::json!({
        "message": format!("Service {} disabled", code),
        "service": item
    }))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_code_accepts_alphanumeric() {
        assert_eq!(normalize_code("WEB_DEV-1").unwrap(), "WEB_DEV-1");
    }

    #[test]
    fn normalize_code_rejects_empty() {
        assert!(normalize_code("  ").is_err());
    }
}
