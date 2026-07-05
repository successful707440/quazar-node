use std::sync::Arc;

use axum::{
    extract::{Extension, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::auth::{AuthContext, KeyStore, Role};
use crate::response::{self, ApiResponse};
use crate::AppState;

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub key: String,
    pub role: String,
    pub citizen_name: String,
}

#[derive(Deserialize)]
pub struct RevokeKeyRequest {
    pub key: String,
}

pub async fn create_api_key(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_api_keys() {
        return response::forbidden("Only Aiya can manage API keys");
    }
    if req.key.trim().len() < 8 {
        return response::bad_request("API key must be at least 8 characters");
    }
    let Some(role) = Role::from_str(&req.role) else {
        return response::bad_request("Invalid role");
    };
    if req.citizen_name.trim().is_empty() {
        return response::bad_request("citizen_name is required");
    }

    match KeyStore::upsert_key(&state.db, req.key.trim(), role, req.citizen_name.trim()).await {
        Ok(()) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key created or updated",
            "key_masked": crate::auth::mask_key(req.key.trim()),
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to save API key: {}", e)),
    }
}

pub async fn list_api_keys(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !auth.can_manage_api_keys() {
        return response::forbidden("Only Aiya can list API keys");
    }

    match KeyStore::list_active(&state.db).await {
        Ok(keys) => Json(ApiResponse::success(serde_json::json!({ "keys": keys }))).into_response(),
        Err(e) => response::internal_error(format!("Failed to list API keys: {}", e)),
    }
}

pub async fn revoke_api_key(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<RevokeKeyRequest>,
) -> impl IntoResponse {
    if !auth.can_manage_api_keys() {
        return response::forbidden("Only Aiya can revoke API keys");
    }
    if req.key.trim().is_empty() {
        return response::bad_request("key is required");
    }

    match KeyStore::revoke_key(&state.db, req.key.trim()).await {
        Ok(true) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key revoked",
            "key_masked": crate::auth::mask_key(req.key.trim()),
        })))
        .into_response(),
        Ok(false) => response::bad_request("API key not found or already revoked"),
        Err(e) => response::internal_error(format!("Failed to revoke API key: {}", e)),
    }
}
