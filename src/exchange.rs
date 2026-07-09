use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::AuthContext;
use crate::response::ApiResponse;
use crate::svod;
use crate::AppState;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OfferStatus {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "filled")]
    Filled,
    #[serde(rename = "cancelled")]
    Cancelled,
}

impl OfferStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OfferStatus::Active => "active",
            OfferStatus::Filled => "filled",
            OfferStatus::Cancelled => "cancelled",
        }
    }
}

impl From<&str> for OfferStatus {
    fn from(s: &str) -> Self {
        match s {
            "active" => OfferStatus::Active,
            "filled" => OfferStatus::Filled,
            "cancelled" => OfferStatus::Cancelled,
            _ => OfferStatus::Active,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct Offer {
    pub id: String,
    pub seller: String,
    pub service: String,
    pub svod_code: Option<String>,
    pub price: i64,
    pub quantity: i64,
    pub status: String,
    pub created_at: i64,
}

impl Offer {
    fn into_json(self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "seller": self.seller,
            "service": self.service,
            "svod_code": self.svod_code,
            "price": self.price,
            "quantity": self.quantity,
            "status": self.status,
            "created_at": self.created_at,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct Order {
    pub id: String,
    pub buyer: String,
    pub offer_id: String,
    pub quantity: i64,
    pub total_price: i64,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateOfferRequest {
    pub svod_code: String,
    pub price: u64,
    pub quantity: u64,
}

#[derive(Debug, Deserialize)]
pub struct CreateOrderRequest {
    pub offer_id: String,
    pub quantity: u64,
}

#[derive(Debug, Deserialize)]
pub struct AddBalanceRequest {
    pub citizen_id: String,
    pub amount: u64,
}

#[derive(Debug, Deserialize)]
pub struct GetOffersQuery {
    pub status: Option<String>,
    pub service: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetOrdersQuery {
    pub status: Option<String>,
}

#[derive(Debug)]
pub enum ExchangeError {
    DatabaseError(String),
    NotFound(String),
    InsufficientBalance,
    InsufficientQuantity,
    Unauthorized(String),
    InvalidStatus(String),
    BadRequest(String),
}

impl ExchangeError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ExchangeError::NotFound(_) => StatusCode::NOT_FOUND,
            ExchangeError::InsufficientBalance => StatusCode::BAD_REQUEST,
            ExchangeError::InsufficientQuantity => StatusCode::BAD_REQUEST,
            ExchangeError::Unauthorized(_) => StatusCode::FORBIDDEN,
            ExchangeError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ExchangeError::InvalidStatus(_) => StatusCode::BAD_REQUEST,
            ExchangeError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ExchangeError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        (
            status,
            Json(ApiResponse::error(self.to_string())),
        )
            .into_response()
    }
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            ExchangeError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ExchangeError::InsufficientBalance => write!(f, "Insufficient balance"),
            ExchangeError::InsufficientQuantity => write!(f, "Insufficient quantity available"),
            ExchangeError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ExchangeError::InvalidStatus(msg) => write!(f, "Invalid status: {}", msg),
            ExchangeError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
        }
    }
}

impl std::error::Error for ExchangeError {}

type ExchangeResult<T> = Result<T, ExchangeError>;

async fn get_balance(pool: &PgPool, citizen_id: &str) -> Result<u64, ExchangeError> {
    let amount: Option<i64> = sqlx::query_scalar(
        "SELECT amount FROM balances WHERE citizen_id = $1",
    )
    .bind(citizen_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    match amount {
        Some(value) => Ok(value as u64),
        None => {
            sqlx::query("INSERT INTO balances (citizen_id, amount) VALUES ($1, 0)")
                .bind(citizen_id)
                .execute(pool)
                .await
                .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
            Ok(0)
        }
    }
}

async fn lock_balance_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    citizen_id: &str,
) -> Result<i64, ExchangeError> {
    sqlx::query(
        "INSERT INTO balances (citizen_id, amount) VALUES ($1, 0)
         ON CONFLICT (citizen_id) DO NOTHING",
    )
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    sqlx::query_scalar("SELECT amount FROM balances WHERE citizen_id = $1 FOR UPDATE")
        .bind(citizen_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))
}

async fn ensure_active_citizen(pool: &PgPool, citizen_id: &str) -> Result<(), ExchangeError> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(citizen_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    match status.as_deref() {
        Some("active") => Ok(()),
        Some("pending") => Err(ExchangeError::BadRequest(
            "Биржа недоступна: паспорт ещё не выдан (статус pending)".to_string(),
        )),
        Some(other) => Err(ExchangeError::BadRequest(format!(
            "Биржа недоступна: статус {other}"
        ))),
        None => Err(ExchangeError::NotFound("Citizen not found".to_string())),
    }
}

pub async fn create_offer(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateOfferRequest>,
) -> ExchangeResult<Json<ApiResponse>> {
    if auth.is_node {
        return Err(ExchangeError::Unauthorized(
            "Node credentials cannot create offers".to_string(),
        ));
    }
    let seller = auth.resolve_account_id(&state.db).await;
    ensure_active_citizen(&state.db, &seller).await?;

    if request.svod_code.trim().is_empty() {
        return Err(ExchangeError::BadRequest("svod_code is required".to_string()));
    }
    if request.price == 0 {
        return Err(ExchangeError::BadRequest("Price must be greater than 0".to_string()));
    }
    if request.quantity == 0 {
        return Err(ExchangeError::BadRequest("Quantity must be greater than 0".to_string()));
    }

    let catalog_item =
        svod::get_service_by_code(&state.db, &request.svod_code, true).await.map_err(|e| {
            ExchangeError::BadRequest(format!("Service not in Svod catalog: {e}"))
        })?;

    if request.price < catalog_item.base_price as u64 {
        return Err(ExchangeError::BadRequest(format!(
            "Price must be at least {} QZ (base_price from Svod)",
            catalog_item.base_price
        )));
    }
    if request.quantity < catalog_item.min_quantity as u64 {
        return Err(ExchangeError::BadRequest(format!(
            "Quantity must be at least {} (Svod min_quantity)",
            catalog_item.min_quantity
        )));
    }
    if request.quantity > catalog_item.max_quantity as u64 {
        return Err(ExchangeError::BadRequest(format!(
            "Quantity must be at most {} (Svod max_quantity)",
            catalog_item.max_quantity
        )));
    }

    let offer_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().timestamp();

    sqlx::query(
        "INSERT INTO offers (id, seller, service, svod_code, price, quantity, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&offer_id)
    .bind(&seller)
    .bind(&catalog_item.name)
    .bind(&catalog_item.code)
    .bind(request.price as i64)
    .bind(request.quantity as i64)
    .bind(OfferStatus::Active.as_str())
    .bind(created_at)
    .execute(&state.db)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    Ok(Json(ApiResponse::success(serde_json::json!({
        "offer_id": offer_id,
        "svod_code": catalog_item.code,
        "service": catalog_item.name,
        "message": "Offer created successfully"
    }))))
}

pub async fn get_offers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<GetOffersQuery>,
) -> ExchangeResult<Json<ApiResponse>> {
    let offers = match (&query.status, &query.service) {
        (Some(status), Some(service)) => {
            let pattern = format!("%{}%", service);
            sqlx::query_as::<_, Offer>(
                "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers
                 WHERE status = $1 AND service LIKE $2 ORDER BY created_at DESC",
            )
            .bind(status)
            .bind(pattern)
            .fetch_all(&state.db)
            .await
        }
        (Some(status), None) => {
            sqlx::query_as::<_, Offer>(
                "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers
                 WHERE status = $1 ORDER BY created_at DESC",
            )
            .bind(status)
            .fetch_all(&state.db)
            .await
        }
        (None, Some(service)) => {
            let pattern = format!("%{}%", service);
            sqlx::query_as::<_, Offer>(
                "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers
                 WHERE service LIKE $1 ORDER BY created_at DESC",
            )
            .bind(pattern)
            .fetch_all(&state.db)
            .await
        }
        (None, None) => {
            sqlx::query_as::<_, Offer>(
                "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers
                 ORDER BY created_at DESC",
            )
            .fetch_all(&state.db)
            .await
        }
    }
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    let data: Vec<_> = offers.into_iter().map(|o| o.into_json()).collect();

    Ok(Json(ApiResponse::success(data)))
}

pub async fn get_offer_by_id(
    State(state): State<Arc<AppState>>,
    Path(offer_id): Path<String>,
) -> ExchangeResult<Json<ApiResponse>> {
    let offer = sqlx::query_as::<_, Offer>(
        "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers WHERE id = $1",
    )
    .bind(&offer_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?
    .ok_or_else(|| ExchangeError::NotFound(format!("Offer {} not found", offer_id)))?;

    Ok(Json(ApiResponse::success(offer.into_json())))
}

pub async fn cancel_offer(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Path(offer_id): Path<String>,
) -> ExchangeResult<Json<ApiResponse>> {
    if auth.is_node {
        return Err(ExchangeError::Unauthorized(
            "Node credentials cannot cancel offers".to_string(),
        ));
    }
    let account_id = auth.resolve_account_id(&state.db).await;
    ensure_active_citizen(&state.db, &account_id).await?;

    let offer = sqlx::query_as::<_, Offer>(
        "SELECT id, seller, service, svod_code, price, quantity, status, created_at FROM offers WHERE id = $1",
    )
    .bind(&offer_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?
    .ok_or_else(|| ExchangeError::NotFound(format!("Offer {} not found", offer_id)))?;

    if !auth.is_master && offer.seller != account_id && offer.seller != auth.citizen_name {
        return Err(ExchangeError::Unauthorized(
            "Only the seller can cancel this offer".to_string(),
        ));
    }

    if offer.status == OfferStatus::Filled.as_str() {
        return Err(ExchangeError::InvalidStatus(
            "Cannot cancel a filled offer".to_string(),
        ));
    }

    sqlx::query("UPDATE offers SET status = $1 WHERE id = $2")
        .bind(OfferStatus::Cancelled.as_str())
        .bind(&offer_id)
        .execute(&state.db)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    Ok(Json(ApiResponse::success(serde_json::json!({
        "message": "Offer cancelled successfully"
    }))))
}

pub async fn create_order(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateOrderRequest>,
) -> ExchangeResult<Json<ApiResponse>> {
    if auth.is_node {
        return Err(ExchangeError::Unauthorized(
            "Node credentials cannot create orders".to_string(),
        ));
    }
    let buyer = auth.resolve_account_id(&state.db).await;
    ensure_active_citizen(&state.db, &buyer).await?;

    if request.quantity == 0 {
        return Err(ExchangeError::BadRequest("Quantity must be greater than 0".to_string()));
    }

    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    let offer = sqlx::query_as::<_, Offer>(
        "SELECT id, seller, service, svod_code, price, quantity, status, created_at
         FROM offers WHERE id = $1 AND status = 'active' FOR UPDATE",
    )
    .bind(&request.offer_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?
    .ok_or_else(|| {
        ExchangeError::NotFound(format!("Active offer {} not found", request.offer_id))
    })?;

    if offer.seller == buyer {
        return Err(ExchangeError::BadRequest("Cannot buy your own offer".to_string()));
    }
    if request.quantity as i64 > offer.quantity {
        return Err(ExchangeError::InsufficientQuantity);
    }

    let total_price = (request.quantity as i64) * offer.price;

    let buyer_balance = lock_balance_in_tx(&mut tx, &buyer).await?;
    if buyer_balance < total_price {
        return Err(ExchangeError::InsufficientBalance);
    }

    lock_balance_in_tx(&mut tx, &offer.seller).await?;

    let order_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().timestamp();
    let new_quantity = offer.quantity - request.quantity as i64;
    let new_status = if new_quantity == 0 {
        OfferStatus::Filled.as_str()
    } else {
        OfferStatus::Active.as_str()
    };

    sqlx::query("UPDATE balances SET amount = amount - $1 WHERE citizen_id = $2")
        .bind(total_price)
        .bind(&buyer)
        .execute(&mut *tx)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    sqlx::query("UPDATE balances SET amount = amount + $1 WHERE citizen_id = $2")
        .bind(total_price)
        .bind(&offer.seller)
        .execute(&mut *tx)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    sqlx::query("UPDATE offers SET quantity = $1, status = $2 WHERE id = $3")
        .bind(new_quantity)
        .bind(new_status)
        .bind(&offer.id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    sqlx::query(
        "INSERT INTO orders (id, buyer, offer_id, quantity, total_price, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&order_id)
    .bind(&buyer)
    .bind(&offer.id)
    .bind(request.quantity as i64)
    .bind(total_price)
    .bind("completed")
    .bind(created_at)
    .execute(&mut *tx)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    Ok(Json(ApiResponse::success(serde_json::json!({
        "order_id": order_id,
        "message": format!("Order created successfully. Total: {} QUAZAR", total_price)
    }))))
}

pub async fn get_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<GetOrdersQuery>,
) -> ExchangeResult<Json<ApiResponse>> {
    if auth.is_node {
        return Err(ExchangeError::Unauthorized(
            "Node credentials cannot list orders".to_string(),
        ));
    }
    let citizen_id = auth.resolve_account_id(&state.db).await;

    let orders = if let Some(status) = query.status {
        sqlx::query_as::<_, Order>(
            "SELECT id, buyer, offer_id, quantity, total_price, status, created_at FROM orders
             WHERE buyer = $1 AND status = $2 ORDER BY created_at DESC",
        )
        .bind(&citizen_id)
        .bind(status)
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, Order>(
            "SELECT id, buyer, offer_id, quantity, total_price, status, created_at FROM orders
             WHERE buyer = $1 ORDER BY created_at DESC",
        )
        .bind(&citizen_id)
        .fetch_all(&state.db)
        .await
    }
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    Ok(Json(ApiResponse::success(orders)))
}

pub async fn get_balance_handler(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
) -> ExchangeResult<Json<ApiResponse>> {
    if auth.is_node {
        return Err(ExchangeError::Unauthorized(
            "Node credentials cannot read balance".to_string(),
        ));
    }
    let citizen_id = auth.resolve_account_id(&state.db).await;
    let balance = get_balance(&state.db, &citizen_id).await?;

    Ok(Json(ApiResponse::success(serde_json::json!({
        "citizen_id": citizen_id,
        "amount": balance
    }))))
}

async fn resolve_citizen_id(pool: &PgPool, citizen_ref: &str) -> Result<String, ExchangeError> {
    if let Some(id) = sqlx::query_scalar("SELECT id FROM citizens WHERE id = $1")
        .bind(citizen_ref)
        .fetch_optional(pool)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?
    {
        return Ok(id);
    }
    if let Some(id) = sqlx::query_scalar("SELECT id FROM citizens WHERE name = $1")
        .bind(citizen_ref)
        .fetch_optional(pool)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?
    {
        return Ok(id);
    }
    Err(ExchangeError::NotFound(format!("Citizen not found: {}", citizen_ref)))
}

pub async fn add_balance(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<AddBalanceRequest>,
) -> ExchangeResult<Json<ApiResponse>> {
    if !auth.can_add_balance() {
        return Err(ExchangeError::Unauthorized(
            "Only Aiya can add balance".to_string(),
        ));
    }
    if request.amount == 0 {
        return Err(ExchangeError::BadRequest("Amount must be greater than 0".to_string()));
    }

    let citizen_id = resolve_citizen_id(&state.db, &request.citizen_id).await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM balances WHERE citizen_id = $1)",
    )
    .bind(&citizen_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    if !exists {
        sqlx::query("INSERT INTO balances (citizen_id, amount) VALUES ($1, 0)")
            .bind(&citizen_id)
            .execute(&state.db)
            .await
            .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    }

    sqlx::query("UPDATE balances SET amount = amount + $1 WHERE citizen_id = $2")
        .bind(request.amount as i64)
        .bind(&citizen_id)
        .execute(&state.db)
        .await
        .map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;

    let new_balance = get_balance(&state.db, &citizen_id).await?;

    Ok(Json(ApiResponse::success(serde_json::json!({
        "citizen_id": citizen_id,
        "added": request.amount,
        "new_balance": new_balance
    }))))
}
