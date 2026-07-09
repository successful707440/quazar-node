use std::sync::Arc;

use axum::{
    extract::{Extension, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::auth::{AuthContext, KeyStore, Role};
use crate::response::{self, ApiResponse};
use crate::AppState;

pub fn hash_password(password: &str) -> String {
    bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("hash password")
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub name: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SetPasswordRequest {
    pub password: String,
}

#[derive(Deserialize)]
pub struct CheckPasswordQuery {
    pub name: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub citizen_id: String,
    pub name: String,
    pub role: String,
    pub api_key: String,
}

#[derive(Serialize)]
pub struct CheckPasswordResponse {
    pub has_password: bool,
}

#[derive(FromRow)]
struct CitizenRow {
    id: String,
    name: String,
    role: String,
    status: String,
}

async fn find_citizen_by_name(pool: &sqlx::PgPool, name: &str) -> Option<CitizenRow> {
    sqlx::query_as::<_, CitizenRow>(
        "SELECT id, name, role, status FROM citizens WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

async fn get_password_hash(pool: &sqlx::PgPool, citizen_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT password_hash FROM citizen_credentials WHERE citizen_id = $1")
        .bind(citizen_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn find_active_api_key(pool: &sqlx::PgPool, citizen_name: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT key FROM api_keys WHERE citizen_name = $1 AND is_active = TRUE ORDER BY created_at LIMIT 1",
    )
    .bind(citizen_name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

fn generate_api_key() -> String {
    format!("qz_{}", uuid::Uuid::new_v4().simple())
}

pub async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let name = req.name.trim();
    let password = req.password.trim();

    if name.is_empty() || password.is_empty() {
        return response::bad_request("Имя и пароль обязательны");
    }

    let Some(citizen) = find_citizen_by_name(&state.db, name).await else {
        return response::bad_request("Гражданин не найден");
    };

    if citizen.status != "active" {
        let message = if citizen.status == "pending" {
            "Вход недоступен: паспорт ещё не выдан (статус pending). Обратитесь к регистратору."
        } else {
            return response::bad_request(format!(
                "Вход недоступен: статус {}",
                citizen.status
            ));
        };
        return response::bad_request(message);
    }

    let Some(hash) = get_password_hash(&state.db, &citizen.id).await else {
        return response::bad_request("Пароль не задан. Используйте API-ключ");
    };

    if !verify_password(password, &hash) {
        return response::bad_request("Неверный пароль");
    }

    let api_key = match find_active_api_key(&state.db, &citizen.name).await {
        Some(key) => key,
        None => {
            let role = Role::from_str(&citizen.role).unwrap_or(Role::Citizen);
            let new_key = generate_api_key();
            if let Err(e) = KeyStore::upsert_key(&state.db, &new_key, role, &citizen.name).await {
                return response::internal_error(format!("Не удалось создать API-ключ: {}", e));
            }
            new_key
        }
    };

    Json(ApiResponse::success(LoginResponse {
        citizen_id: citizen.id,
        name: citizen.name,
        role: citizen.role,
        api_key,
    }))
    .into_response()
}

pub async fn set_password_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetPasswordRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden("Node credentials cannot set passwords");
    }

    let password = req.password.trim();
    if password.len() < 6 {
        return response::bad_request("Пароль должен быть не короче 6 символов");
    }

    let citizen_id = auth.resolve_account_id(&state.db).await;
    let password_hash = hash_password(password);

    let result = sqlx::query(
        r#"
        INSERT INTO citizen_credentials (citizen_id, password_hash, created_at, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (citizen_id) DO UPDATE SET
            password_hash = EXCLUDED.password_hash,
            updated_at = NOW()
        "#,
    )
    .bind(&citizen_id)
    .bind(&password_hash)
    .execute(&state.db)
    .await;

    match result {
        Ok(_) => Json(ApiResponse::success(serde_json::json!({
            "message": "Пароль сохранён",
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Не удалось сохранить пароль: {}", e)),
    }
}

const TEST_CITIZEN_NAMES: &[&str] = &["testcitizen", "buyercitizen", "sellercitizen"];

/// When `QUAZAR_INIT_TEST_KEYS=true`, ensure seed test citizens have a default password
/// on this node's PostgreSQL (each node has its own DB; credentials are not P2P-replicated).
pub async fn sync_test_passwords(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    let enabled = std::env::var("QUAZAR_INIT_TEST_KEYS")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if !enabled {
        return Ok(());
    }

    let password = std::env::var("QUAZAR_TEST_CITIZEN_PASSWORD")
        .unwrap_or_else(|_| "123456".to_string());

    for name in TEST_CITIZEN_NAMES {
        let Some(citizen) = find_citizen_by_name(pool, name).await else {
            continue;
        };

        if get_password_hash(pool, &citizen.id).await.is_some() {
            continue;
        }

        let password_hash = hash_password(&password);
        sqlx::query(
            r#"
            INSERT INTO citizen_credentials (citizen_id, password_hash, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (citizen_id) DO NOTHING
            "#,
        )
        .bind(&citizen.id)
        .bind(&password_hash)
        .execute(pool)
        .await?;

        tracing::info!(citizen = %name, "test citizen default password synced");
    }

    Ok(())
}

pub async fn check_password_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CheckPasswordQuery>,
) -> impl IntoResponse {
    let name = query.name.trim();
    if name.is_empty() {
        return response::bad_request("Параметр name обязателен");
    }

    let Some(citizen) = find_citizen_by_name(&state.db, name).await else {
        return response::bad_request("Гражданин не найден");
    };

    let has_password = get_password_hash(&state.db, &citizen.id)
        .await
        .is_some();

    Json(ApiResponse::success(CheckPasswordResponse { has_password })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_password_roundtrip() {
        let hash = hash_password("secret123");
        assert!(verify_password("secret123", &hash));
        assert!(!verify_password("wrong", &hash));
    }

    #[test]
    fn verify_password_rejects_invalid_hash() {
        assert!(!verify_password("secret", "not-a-bcrypt-hash"));
    }
}
