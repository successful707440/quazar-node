use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::num::NonZeroU32;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use governor::{Quota, RateLimiter};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::auth::{is_node_secret, master_key, AuthContext, KeyStore};
use crate::response::{self, ApiResponse};
use crate::types::Role;
use crate::AppState;

type KeyedLimiter = RateLimiter<
    String,
    governor::state::keyed::DashMapStateStore<String>,
    governor::clock::DefaultClock,
>;

static RATE_LIMITER: OnceLock<Arc<KeyedLimiter>> = OnceLock::new();

fn mask_key(key: &str) -> String {
    if key.is_empty() {
        return "<empty>".to_string();
    }
    if key.len() <= 8 {
        return "***".to_string();
    }
    format!("{}...{}", &key[..4], &key[key.len() - 4..])
}

fn rate_limiter() -> Arc<KeyedLimiter> {
    RATE_LIMITER
        .get_or_init(|| {
            let rps = std::env::var("QUAZAR_RATE_LIMIT_RPS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);
            let rps = NonZeroU32::new(rps.max(1)).unwrap_or(NonZeroU32::MIN);
            Arc::new(RateLimiter::keyed(Quota::per_second(rps)))
        })
        .clone()
}

fn client_ip(req: &axum::http::Request<axum::body::Body>) -> String {
    if let Some(forwarded) = req.headers().get("x-forwarded-for") {
        if let Ok(value) = forwarded.to_str() {
            if let Some(first) = value.split(',').next() {
                let ip = first.trim();
                if !ip.is_empty() {
                    return ip.to_string();
                }
            }
        }
    }
    if let Some(real_ip) = req.headers().get("x-real-ip") {
        if let Ok(ip) = real_ip.to_str() {
            if !ip.trim().is_empty() {
                return ip.trim().to_string();
            }
        }
    }
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn strict_secrets_enabled() -> bool {
    std::env::var("QUAZAR_STRICT_SECRETS")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

fn default_dev_cors_origins() -> Vec<HeaderValue> {
    [
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:8081",
        "http://127.0.0.1:8081",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    .iter()
    .filter_map(|origin| origin.parse().ok())
    .collect()
}

pub fn build_cors_layer() -> CorsLayer {
    match std::env::var("QUAZAR_CORS_ORIGINS") {
        Ok(origins) if origins.trim() == "*" => {
            tracing::warn!("CORS permissive (QUAZAR_CORS_ORIGINS=*)");
            CorsLayer::permissive()
        }
        Ok(origins) if !origins.trim().is_empty() => {
            let allowed: Vec<HeaderValue> = origins
                .split(',')
                .filter_map(|origin| origin.trim().parse().ok())
                .collect();
            if allowed.is_empty() {
                tracing::warn!("QUAZAR_CORS_ORIGINS is empty after parsing, using dev defaults");
                return CorsLayer::new()
                    .allow_origin(AllowOrigin::list(default_dev_cors_origins()))
                    .allow_methods(Any)
                    .allow_headers(Any);
            }
            tracing::info!(count = allowed.len(), "CORS restricted to configured origins");
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(allowed))
                .allow_methods(Any)
                .allow_headers(Any)
        }
        _ if strict_secrets_enabled() => {
            tracing::warn!(
                "QUAZAR_STRICT_SECRETS: cross-origin CORS disabled until QUAZAR_CORS_ORIGINS is set"
            );
            CorsLayer::new().allow_methods(Any).allow_headers(Any)
        }
        _ => {
            tracing::info!("CORS dev defaults (set QUAZAR_CORS_ORIGINS or * for other modes)");
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(default_dev_cors_origins()))
                .allow_methods(Any)
                .allow_headers(Any)
        }
    }
}

fn enforce_path_rbac(path: &str, auth: &AuthContext) -> Option<Response> {
    if (path == "/peers" || path == "/peers/network") && !auth.can_manage_peers() {
        return Some(response::forbidden(
            "Insufficient permissions. Only Aiya and Guardian can manage peers",
        ));
    }
    if path == "/exchange/balance/add" && !auth.can_add_balance() {
        return Some(response::forbidden(
            "Insufficient permissions. Only Aiya can add balance",
        ));
    }
    None
}

pub async fn rate_limit_middleware(req: axum::http::Request<axum::body::Body>, next: Next) -> Response {
    let ip = client_ip(&req);
    if rate_limiter().check_key(&ip).is_err() {
        tracing::warn!(path = %req.uri().path(), client_ip = %ip, "rate limit exceeded");
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse::error("Rate limit exceeded")),
        )
            .into_response();
    }
    next.run(req).await
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let x_api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    tracing::debug!(
        path = %path,
        authorization = ?auth_header.as_deref().map(mask_key),
        x_api_key = ?x_api_key.as_deref().map(mask_key),
        "auth request"
    );

    let api_key = auth_header
        .as_deref()
        .and_then(|v| {
            if v.starts_with("Bearer ") {
                Some(v[7..].to_string())
            } else {
                None
            }
        })
        .or(x_api_key)
        .unwrap_or_default();

    if api_key.is_empty() {
        tracing::warn!(path = %path, "missing API key");
        return response::unauthorized("Invalid or missing API key");
    }

    if api_key == master_key() {
        if std::env::var("QUAZAR_DISABLE_MASTER_KEY")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
        {
            tracing::warn!(path = %path, "master key rejected (QUAZAR_DISABLE_MASTER_KEY)");
            return response::unauthorized("Master key is disabled on this node");
        }
        tracing::debug!(path = %path, "master key accepted");
        let auth = AuthContext::master();
        if let Some(response) = enforce_path_rbac(&path, &auth) {
            return response;
        }
        req.extensions_mut().insert(auth);
        return next.run(req).await;
    }

    if is_node_secret(&api_key) {
        let method = req.method();
        let node_allowed = *method == Method::GET
            && (path == "/events"
                || path == "/blocks"
                || path == "/keys/internal/export")
            || *method == Method::POST
                && (path == "/events/gossip"
                    || path == "/keys/internal/upsert"
                    || path == "/keys/internal/revoke");
        if node_allowed {
            tracing::debug!(path = %path, "node secret accepted");
            req.extensions_mut().insert(AuthContext::node());
            return next.run(req).await;
        }
        tracing::warn!(path = %path, method = %method, "node secret rejected for route");
        return response::forbidden("Node secret valid only for GET /events, GET /blocks, GET /keys/internal/export and POST /events/gossip, /keys/internal/*");
    }

    let key_data = KeyStore::validate_key(&state.db, &api_key)
        .await
        .map(|k| (k.citizen_name, k.role));

    if let Some((citizen_name, key_role)) = key_data {
        let role = resolve_role_for_citizen(&state.db, &citizen_name, key_role).await;
        tracing::debug!(path = %path, citizen = %citizen_name, ?role, "API key accepted");
        let auth = AuthContext::from_api_key(citizen_name, role);
        if let Some(response) = enforce_path_rbac(&path, &auth) {
            return response;
        }
        req.extensions_mut().insert(auth);
        return next.run(req).await;
    }

    tracing::warn!(path = %path, key = %mask_key(&api_key), "API key not found");
    response::unauthorized("Invalid or missing API key")
}

async fn resolve_role_for_citizen(
    pool: &sqlx::PgPool,
    citizen_name: &str,
    key_role: Role,
) -> Role {
    let db_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM citizens WHERE name = $1",
    )
    .bind(citizen_name)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    db_role
        .and_then(|r| Role::from_str(&r))
        .unwrap_or(key_role)
}
