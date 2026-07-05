use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    middleware,
    routing::{delete, get, patch, post},
    Router,
};
use chrono::Utc;
use sqlx::PgPool;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

mod auth;
mod block_producer;
mod blockchain;
mod citizen;
mod db;
mod exchange;
mod crypto;
mod gossip;
mod handlers;
mod http;
mod keys;
mod models;
mod nodes;
mod pending;
mod projection;
mod response;
mod types;
mod validator;
mod votes;

#[cfg(test)]
mod integration_tests;

use auth::{init_master_key, init_node_secret, init_reg_secret, master_key, node_secret, reg_secret, KeyStore};
use crypto::assert_production_secrets;
use citizen::*;
use handlers::background_sync;
use http::{auth_middleware, build_cors_layer, rate_limit_middleware};
use nodes::{Node, NodeRegistry, NodeStatus};

pub use models::{Block, Event};

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    node_registry: Arc<NodeRegistry>,
    pub node_id: String,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    init_tracing();
    init_master_key();
    init_node_secret();
    init_reg_secret();
    assert_production_secrets(master_key(), node_secret(), reg_secret());

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://quazar:quazar@localhost:5432/quazar".to_string());
    let node_id = std::env::var("QUAZAR_NODE_ID").unwrap_or_else(|_| "QZ-NODE".to_string());
    let node_url = std::env::var("QUAZAR_NODE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let port = std::env::var("QUAZAR_PORT").unwrap_or_else(|_| "8080".to_string());

    tracing::info!("Quazar Blockchain v0.7.0 starting");

    let pool = db::connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");
    db::run_migrations(&pool)
        .await
        .expect("Failed to run database migrations");
    KeyStore::sync_from_env(&pool)
        .await
        .expect("Failed to sync API keys");
    let pending_count = pending::count(&pool).await.unwrap_or(0);
    tracing::info!(pending_events = pending_count, "PostgreSQL connected");

    let node_registry = Arc::new(NodeRegistry::new(pool.clone()));

    let my_node = Node {
        id: node_id.clone(),
        url: node_url.clone(),
        public_key: None,
        status: NodeStatus::Alive,
        last_seen: Utc::now(),
        version: "0.7.0".to_string(),
    };
    let _ = node_registry.upsert_node(&my_node).await;

    if let Ok(bootstrap) = std::env::var("QUAZAR_BOOTSTRAP_PEERS") {
        for entry in bootstrap.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((id, url)) = entry.split_once('@') else {
                tracing::warn!(entry = %entry, "invalid bootstrap peer format (use id@url)");
                continue;
            };
            if id == node_id {
                continue;
            }
            let peer = Node {
                id: id.to_string(),
                url: url.to_string(),
                public_key: None,
                status: NodeStatus::Alive,
                last_seen: Utc::now(),
                version: "0.7.0".to_string(),
            };
            match node_registry.upsert_node(&peer).await {
                Ok(()) => tracing::info!(peer_id = %id, url = %url, "bootstrap peer registered"),
                Err(e) => tracing::warn!(peer_id = %id, error = %e, "failed to register bootstrap peer"),
            }
        }
    }

    let state = Arc::new(AppState {
        db: pool,
        node_registry,
        node_id,
    });

    let state_clone = state.clone();
    tokio::spawn(async move {
        background_sync(state_clone).await;
    });

    let protected_routes = Router::new()
        .route("/blocks", get(handlers::get_blocks))
        .route("/events", get(handlers::get_events))
        .route("/events/gossip", post(handlers::gossip_event))
        .route("/event", post(handlers::add_event))
        .route("/nodes", get(handlers::get_nodes))
        .route("/peers", post(handlers::add_peer))
        .route("/peers/network", post(handlers::add_peer_to_network))
        .route("/online", post(handlers::online_handler))
        .route("/offline", post(handlers::offline_handler))
        .route("/vote", post(handlers::cast_vote_handler))
        .route("/votes", post(votes::create_vote))
        .route("/votes", get(votes::list_votes))
        .route("/votes/finalize", post(votes::finalize_vote))
        .route("/exchange/offer", post(exchange::create_offer))
        .route("/exchange/offers", get(exchange::get_offers))
        .route("/exchange/offer/:id", get(exchange::get_offer_by_id))
        .route("/exchange/offer/:id", delete(exchange::cancel_offer))
        .route("/exchange/order", post(exchange::create_order))
        .route("/exchange/orders", get(exchange::get_orders))
        .route("/exchange/balance", get(exchange::get_balance_handler))
        .route("/exchange/balance/add", post(exchange::add_balance))
        .route("/keys", post(keys::create_api_key))
        .route("/keys", get(keys::list_api_keys))
        .route("/keys/revoke", post(keys::revoke_api_key))
        .route("/citizen/register", post(register_citizen))
        .route("/citizen/list", get(list_citizens))
        .route("/citizen/:id", get(get_citizen))
        .route("/citizen/:id/status", patch(update_status))
        .route("/citizen/:id/passport", post(issue_passport))
        .route("/citizen/:id/passport/revoke", post(revoke_passport))
        .route("/citizen/search", get(search_citizens))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .route_layer(middleware::from_fn(rate_limit_middleware))
        .with_state(state.clone());

    let app = Router::new()
        .route("/status", get(handlers::status))
        .merge(protected_routes)
        .layer(TraceLayer::new_for_http())
        .layer(build_cors_layer())
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    tracing::info!(%addr, "listening");
    axum::serve(
        tokio::net::TcpListener::bind(&addr).await.unwrap(),
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await
        .unwrap();
}
