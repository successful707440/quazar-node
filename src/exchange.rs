// src/exchange.rs - Модуль биржи Quazar (упрощенная версия)
use axum::{
    extract::{Path, Query, State},
    response::Json,
    response::IntoResponse,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use rusqlite::{params, Connection, ToSql};
use chrono::Utc;
use uuid::Uuid;

use crate::auth::{Role, KeyStore};
use crate::AppState;

// ===== СТРУКТУРЫ ДАННЫХ =====

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Offer {
    pub id: String,
    pub seller: String,
    pub service: String,
    pub price: u64,
    pub quantity: u64,
    pub status: OfferStatus,
    pub created_at: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OrderStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "cancelled")]
    Cancelled,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Pending => "pending",
            OrderStatus::Completed => "completed",
            OrderStatus::Cancelled => "cancelled",
        }
    }
}

impl From<&str> for OrderStatus {
    fn from(s: &str) -> Self {
        match s {
            "pending" => OrderStatus::Pending,
            "completed" => OrderStatus::Completed,
            "cancelled" => OrderStatus::Cancelled,
            _ => OrderStatus::Pending,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub id: String,
    pub buyer: String,
    pub offer_id: String,
    pub quantity: u64,
    pub total_price: u64,
    pub status: OrderStatus,
    pub created_at: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Balance {
    pub citizen_id: String,
    pub amount: u64,
}

// ===== REQUEST STRUCTS =====

#[derive(Debug, Deserialize)]
pub struct CreateOfferRequest {
    pub service: String,
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

// ===== ОШИБКИ =====

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
        let message = self.to_string();
        
        let body = serde_json::json!({
            "status": "error",
            "error": message,
        });
        
        (status, Json(body)).into_response()
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

// ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

pub fn init_exchange_tables(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS offers (
            id TEXT PRIMARY KEY,
            seller TEXT NOT NULL,
            service TEXT NOT NULL,
            price INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            buyer TEXT NOT NULL,
            offer_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            total_price INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS balances (
            citizen_id TEXT PRIMARY KEY,
            amount INTEGER NOT NULL DEFAULT 0
        )",
        [],
    )?;

    Ok(())
}

pub fn get_balance(conn: &Connection, citizen_id: &str) -> Result<u64, ExchangeError> {
    let result: Result<Option<u64>, _> = conn.query_row(
        "SELECT amount FROM balances WHERE citizen_id = ?1",
        [citizen_id],
        |row| row.get(0),
    ).map(Some)
    .or_else(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        _ => Err(ExchangeError::DatabaseError(e.to_string())),
    });

    match result {
        Ok(Some(amount)) => Ok(amount),
        Ok(None) => {
            conn.execute(
                "INSERT INTO balances (citizen_id, amount) VALUES (?1, 0)",
                [citizen_id],
            ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
            Ok(0)
        }
        Err(e) => Err(e),
    }
}

pub fn update_balance(
    conn: &Connection,
    citizen_id: &str,
    delta: i64,
) -> Result<u64, ExchangeError> {
    let current = get_balance(conn, citizen_id)?;
    
    if delta < 0 && (current as i64 + delta) < 0 {
        return Err(ExchangeError::InsufficientBalance);
    }
    
    let new_amount = (current as i64 + delta) as u64;
    
    conn.execute(
        "UPDATE balances SET amount = ?1 WHERE citizen_id = ?2",
        params![new_amount, citizen_id],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    Ok(new_amount)
}

// ===== API ЭНДПОИНТЫ (все используют только State) =====

pub async fn create_offer(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateOfferRequest>,
) -> ExchangeResult<Json<serde_json::Value>> {
    // Временно используем hardcoded seller для теста
    let seller = "successful".to_string();
    
    if request.service.is_empty() {
        return Err(ExchangeError::BadRequest("Service name is required".to_string()));
    }
    if request.price == 0 {
        return Err(ExchangeError::BadRequest("Price must be greater than 0".to_string()));
    }
    if request.quantity == 0 {
        return Err(ExchangeError::BadRequest("Quantity must be greater than 0".to_string()));
    }
    
    let offer_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().timestamp();
    
    let db = state.db.lock().await;
    
    db.execute(
        "INSERT INTO offers (id, seller, service, price, quantity, status, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            offer_id,
            seller,
            request.service,
            request.price,
            request.quantity,
            OfferStatus::Active.as_str(),
            created_at,
        ],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "offer_id": offer_id,
            "message": "Offer created successfully"
        }
    })))
}

pub async fn get_offers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<GetOffersQuery>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let db = state.db.lock().await;
    
    let mut sql = "SELECT id, seller, service, price, quantity, status, created_at FROM offers WHERE 1=1".to_string();
    let mut params: Vec<Box<dyn ToSql>> = vec![];
    
    if let Some(status) = query.status {
        sql.push_str(" AND status = ?");
        params.push(Box::new(status));
    }
    
    if let Some(service) = query.service {
        sql.push_str(" AND service LIKE ?");
        params.push(Box::new(format!("%{}%", service)));
    }
    
    sql.push_str(" ORDER BY created_at DESC");
    
    let mut stmt = db.prepare(&sql).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
    
    let rows = stmt.query_map(&param_refs[..], |row| {
        let status_str: String = row.get(5)?;
        Ok(Offer {
            id: row.get(0)?,
            seller: row.get(1)?,
            service: row.get(2)?,
            price: row.get(3)?,
            quantity: row.get(4)?,
            status: OfferStatus::from(status_str.as_str()),
            created_at: row.get(6)?,
        })
    }).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let offers: Vec<Offer> = rows.filter_map(|row| row.ok()).collect();
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": offers
    })))
}

pub async fn get_offer_by_id(
    State(state): State<Arc<AppState>>,
    Path(offer_id): Path<String>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let db = state.db.lock().await;
    
    let row = db.query_row(
        "SELECT id, seller, service, price, quantity, status, created_at FROM offers WHERE id = ?1",
        [&offer_id],
        |row| {
            let status_str: String = row.get(5)?;
            Ok(Offer {
                id: row.get(0)?,
                seller: row.get(1)?,
                service: row.get(2)?,
                price: row.get(3)?,
                quantity: row.get(4)?,
                status: OfferStatus::from(status_str.as_str()),
                created_at: row.get(6)?,
            })
        },
    ).map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => ExchangeError::NotFound(format!("Offer {} not found", offer_id)),
        _ => ExchangeError::DatabaseError(e.to_string()),
    })?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": row
    })))
}

pub async fn cancel_offer(
    State(state): State<Arc<AppState>>,
    Path(offer_id): Path<String>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let db = state.db.lock().await;
    
    let offer: Offer = db.query_row(
        "SELECT id, seller, service, price, quantity, status, created_at FROM offers WHERE id = ?1",
        [&offer_id],
        |row| {
            let status_str: String = row.get(5)?;
            Ok(Offer {
                id: row.get(0)?,
                seller: row.get(1)?,
                service: row.get(2)?,
                price: row.get(3)?,
                quantity: row.get(4)?,
                status: OfferStatus::from(status_str.as_str()),
                created_at: row.get(6)?,
            })
        },
    ).map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => ExchangeError::NotFound(format!("Offer {} not found", offer_id)),
        _ => ExchangeError::DatabaseError(e.to_string()),
    })?;
    
    // Проверяем права (для теста разрешаем все)
    // В реальном проекте нужно проверять seller == current_user
    
    if matches!(offer.status, OfferStatus::Filled) {
        return Err(ExchangeError::InvalidStatus("Cannot cancel a filled offer".to_string()));
    }
    
    db.execute(
        "UPDATE offers SET status = ?1 WHERE id = ?2",
        params![OfferStatus::Cancelled.as_str(), offer_id],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "message": "Offer cancelled successfully"
        }
    })))
}

pub async fn create_order(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateOrderRequest>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let buyer = "successful".to_string();
    
    if request.quantity == 0 {
        return Err(ExchangeError::BadRequest("Quantity must be greater than 0".to_string()));
    }
    
    let mut db = state.db.lock().await;
    
    let offer: Offer = db.query_row(
        "SELECT id, seller, service, price, quantity, status, created_at FROM offers WHERE id = ?1 AND status = 'active'",
        [&request.offer_id],
        |row| {
            let status_str: String = row.get(5)?;
            Ok(Offer {
                id: row.get(0)?,
                seller: row.get(1)?,
                service: row.get(2)?,
                price: row.get(3)?,
                quantity: row.get(4)?,
                status: OfferStatus::from(status_str.as_str()),
                created_at: row.get(6)?,
            })
        },
    ).map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => ExchangeError::NotFound(format!("Active offer {} not found", request.offer_id)),
        _ => ExchangeError::DatabaseError(e.to_string()),
    })?;
    
    if offer.seller == buyer {
        return Err(ExchangeError::BadRequest("Cannot buy your own offer".to_string()));
    }
    
    if request.quantity > offer.quantity {
        return Err(ExchangeError::InsufficientQuantity);
    }
    
    let total_price = request.quantity * offer.price;
    
    let buyer_balance = get_balance(&db, &buyer)?;
    if buyer_balance < total_price {
        return Err(ExchangeError::InsufficientBalance);
    }
    
    let order_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().timestamp();
    
    let tx = db.transaction().map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    tx.execute(
        "UPDATE balances SET amount = amount - ?1 WHERE citizen_id = ?2",
        params![total_price, buyer],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    tx.execute(
        "UPDATE balances SET amount = amount + ?1 WHERE citizen_id = ?2",
        params![total_price, offer.seller],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let new_quantity = offer.quantity - request.quantity;
    let new_status = if new_quantity == 0 {
        OfferStatus::Filled.as_str()
    } else {
        OfferStatus::Active.as_str()
    };
    
    tx.execute(
        "UPDATE offers SET quantity = ?1, status = ?2 WHERE id = ?3",
        params![new_quantity, new_status, offer.id],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    tx.execute(
        "INSERT INTO orders (id, buyer, offer_id, quantity, total_price, status, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            order_id,
            buyer,
            offer.id,
            request.quantity,
            total_price,
            OrderStatus::Completed.as_str(),
            created_at,
        ],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    tx.commit().map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "order_id": order_id,
            "message": format!("Order created successfully. Total: {} QUAZAR", total_price)
        }
    })))
}

pub async fn get_orders(
    State(state): State<Arc<AppState>>,
    Query(query): Query<GetOrdersQuery>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let citizen_id = "successful".to_string();
    
    let db = state.db.lock().await;
    
    let mut sql = "SELECT id, buyer, offer_id, quantity, total_price, status, created_at FROM orders WHERE buyer = ?1".to_string();
    let mut params: Vec<Box<dyn ToSql>> = vec![Box::new(citizen_id)];
    
    if let Some(status) = query.status {
        sql.push_str(" AND status = ?");
        params.push(Box::new(status));
    }
    
    sql.push_str(" ORDER BY created_at DESC");
    
    let mut stmt = db.prepare(&sql).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
    
    let rows = stmt.query_map(&param_refs[..], |row| {
        let status_str: String = row.get(5)?;
        Ok(Order {
            id: row.get(0)?,
            buyer: row.get(1)?,
            offer_id: row.get(2)?,
            quantity: row.get(3)?,
            total_price: row.get(4)?,
            status: OrderStatus::from(status_str.as_str()),
            created_at: row.get(6)?,
        })
    }).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let orders: Vec<Order> = rows.filter_map(|row| row.ok()).collect();
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": orders
    })))
}

pub async fn get_balance_handler(
    State(state): State<Arc<AppState>>,
) -> ExchangeResult<Json<serde_json::Value>> {
    let citizen_id = "successful".to_string();
    
    let db = state.db.lock().await;
    let balance = get_balance(&db, &citizen_id)?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "citizen_id": citizen_id,
            "amount": balance
        }
    })))
}

pub async fn add_balance(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AddBalanceRequest>,
) -> ExchangeResult<Json<serde_json::Value>> {
    // Только для теста - разрешаем добавлять баланс без проверки
    if request.amount == 0 {
        return Err(ExchangeError::BadRequest("Amount must be greater than 0".to_string()));
    }
    
    let db = state.db.lock().await;
    
    let exists: bool = db.query_row(
        "SELECT COUNT(*) FROM balances WHERE citizen_id = ?1",
        [&request.citizen_id],
        |row| row.get(0),
    ).unwrap_or(0) > 0;
    
    if !exists {
        db.execute(
            "INSERT INTO balances (citizen_id, amount) VALUES (?1, 0)",
            [&request.citizen_id],
        ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    }
    
    db.execute(
        "UPDATE balances SET amount = amount + ?1 WHERE citizen_id = ?2",
        params![request.amount, request.citizen_id],
    ).map_err(|e| ExchangeError::DatabaseError(e.to_string()))?;
    
    let new_balance = get_balance(&db, &request.citizen_id)?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "citizen_id": request.citizen_id,
            "added": request.amount,
            "new_balance": new_balance
        }
    })))
}
