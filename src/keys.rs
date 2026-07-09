use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Extension, State},
    response::IntoResponse,
    Json,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::auth::{node_secret, AuthContext, KeyStore, Role};
use crate::response::{self, ApiResponse};
use crate::AppState;

#[derive(Deserialize, Serialize)]
pub struct CreateKeyRequest {
    pub key: String,
    pub role: String,
    pub citizen_name: String,
}

#[derive(Deserialize, Serialize)]
pub struct RevokeKeyRequest {
    pub key: String,
}

#[derive(Serialize, Deserialize)]
struct InternalKeyRecord {
    key: String,
    role: String,
    citizen_name: String,
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
    if req.citizen_name.trim().is_empty() {
        return response::bad_request("citizen_name is required");
    }

    let citizen_name = req.citizen_name.trim();
    let Some(role) = KeyStore::lookup_citizen_role(&state.db, citizen_name).await else {
        return response::bad_request("Citizen not found");
    };

    match KeyStore::upsert_key(&state.db, req.key.trim(), role.clone(), citizen_name).await
    {
        Ok(()) => {
            push_key_to_peers(&state, req.key.trim(), role, citizen_name).await;
            Json(ApiResponse::success(serde_json::json!({
                "message": "API key created or updated",
                "key_masked": crate::auth::mask_key(req.key.trim()),
            })))
            .into_response()
        }
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
        Ok(true) => {
            push_key_revoke_to_peers(&state, req.key.trim()).await;
            Json(ApiResponse::success(serde_json::json!({
                "message": "API key revoked",
                "key_masked": crate::auth::mask_key(req.key.trim()),
            })))
            .into_response()
        }
        Ok(false) => response::bad_request("API key not found or already revoked"),
        Err(e) => response::internal_error(format!("Failed to revoke API key: {}", e)),
    }
}

pub async fn internal_export_keys(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !auth.is_node {
        return response::forbidden("Node credentials required");
    }

    match KeyStore::list_active_full(&state.db).await {
        Ok(keys) => {
            let records: Vec<InternalKeyRecord> = keys
                .into_iter()
                .map(|k| InternalKeyRecord {
                    key: k.key,
                    role: k.role.as_str().to_string(),
                    citizen_name: k.citizen_name,
                })
                .collect();
            Json(ApiResponse::success(records)).into_response()
        }
        Err(e) => response::internal_error(format!("Failed to export API keys: {}", e)),
    }
}

pub async fn sync_api_keys(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !auth.can_manage_api_keys() {
        return response::forbidden("Only Aiya can sync API keys");
    }

    match KeyStore::sync_roles_from_citizens(&state.db).await {
        Ok(updated) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key roles synced from citizens registry",
            "updated": updated,
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to sync API key roles: {}", e)),
    }
}

pub async fn internal_upsert_key(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    if !auth.is_node {
        return response::forbidden("Node credentials required");
    }
    if req.key.trim().is_empty() || req.citizen_name.trim().is_empty() {
        return response::bad_request("key and citizen_name are required");
    }

    let citizen_name = req.citizen_name.trim();
    let role = match KeyStore::lookup_citizen_role(&state.db, citizen_name).await {
        Some(role) => role,
        None => match Role::from_str(&req.role) {
            Some(role) => role,
            None => return response::bad_request("Invalid role"),
        },
    };

    match KeyStore::upsert_key(&state.db, req.key.trim(), role, citizen_name).await {
        Ok(()) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key synced",
            "key_masked": crate::auth::mask_key(req.key.trim()),
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to sync API key: {}", e)),
    }
}

pub async fn internal_revoke_key(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(req): Json<RevokeKeyRequest>,
) -> impl IntoResponse {
    if !auth.is_node {
        return response::forbidden("Node credentials required");
    }
    if req.key.trim().is_empty() {
        return response::bad_request("key is required");
    }

    match KeyStore::revoke_key(&state.db, req.key.trim()).await {
        Ok(true) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key revoked on peer",
            "key_masked": crate::auth::mask_key(req.key.trim()),
        })))
        .into_response(),
        Ok(false) => Json(ApiResponse::success(serde_json::json!({
            "message": "API key already absent",
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to revoke API key: {}", e)),
    }
}

pub async fn push_key_to_peers(state: &AppState, key: &str, role: Role, citizen_name: &str) {
    let peers = match state.node_registry.get_all_nodes().await {
        Ok(peers) => peers,
        Err(e) => {
            tracing::warn!(error = %e, "key sync: failed to load peers");
            return;
        }
    };

    let body = CreateKeyRequest {
        key: key.to_string(),
        role: role.as_str().to_string(),
        citizen_name: citizen_name.to_string(),
    };

    let client = Client::new();
    for peer in peers {
        if peer.id == state.node_id {
            continue;
        }
        let url = format!(
            "{}/keys/internal/upsert",
            peer.url.trim_end_matches('/')
        );
        match client
            .post(&url)
            .header("Authorization", format!("Bearer {}", node_secret()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(peer = %peer.id, key = %crate::auth::mask_key(key), "API key pushed to peer");
            }
            Ok(resp) => {
                tracing::warn!(
                    peer = %peer.id,
                    status = %resp.status(),
                    "API key push rejected by peer"
                );
            }
            Err(e) => tracing::warn!(peer = %peer.id, error = %e, "API key push failed"),
        }
    }
}

async fn push_key_revoke_to_peers(state: &AppState, key: &str) {
    let peers = match state.node_registry.get_all_nodes().await {
        Ok(peers) => peers,
        Err(e) => {
            tracing::warn!(error = %e, "key revoke sync: failed to load peers");
            return;
        }
    };

    let body = RevokeKeyRequest {
        key: key.to_string(),
    };
    let client = Client::new();
    for peer in peers {
        if peer.id == state.node_id {
            continue;
        }
        let url = format!(
            "{}/keys/internal/revoke",
            peer.url.trim_end_matches('/')
        );
        let _ = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", node_secret()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await;
    }
}

pub async fn sync_keys_from_peers(state: &AppState) {
    let peers = match state.node_registry.get_all_nodes().await {
        Ok(peers) => peers,
        Err(_) => return,
    };

    let client = Client::new();
    for peer in peers {
        if peer.id == state.node_id {
            continue;
        }
        let url = format!(
            "{}/keys/internal/export",
            peer.url.trim_end_matches('/')
        );
        let Ok(resp) = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", node_secret()))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        else {
            continue;
        };
        let Ok(body) = resp.json::<ApiResponse>().await else {
            continue;
        };
        let Some(records) = crate::response::decode_data::<Vec<InternalKeyRecord>>(&body) else {
            continue;
        };
        for record in records {
            let Some(role) = Role::from_str(&record.role) else {
                continue;
            };
            if KeyStore::upsert_key(&state.db, &record.key, role, &record.citizen_name)
                .await
                .is_ok()
            {
                tracing::debug!(
                    peer = %peer.id,
                    key = %crate::auth::mask_key(&record.key),
                    citizen = %record.citizen_name,
                    "API key synced from peer"
                );
            }
        }
    }
}
