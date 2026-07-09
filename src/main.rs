use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    middleware,
    routing::{delete, get, patch, post, put},
    Router,
};
use chrono::Utc;
use sqlx::PgPool;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

mod auth;
mod auth_login;
mod block_producer;
mod blockchain;
mod candidacy;
mod chat;
mod citizen;
mod db;
mod exchange;
mod crypto;
mod gossip;
mod handlers;
mod http;
mod initiative;
mod keys;
mod models;
mod nodes;
mod pending;
mod projection;
mod referendum;
mod response;
mod svod;
mod types;
mod validator;
mod votes;

#[cfg(test)]
mod integration_tests;

use auth::{init_master_key, init_node_secret, init_reg_secret, master_key, node_secret, reg_secret, KeyStore};
use auth_login::{check_password_handler, login_handler, set_password_handler, sync_test_passwords};
use crypto::assert_production_secrets;
use citizen::*;
use candidacy::{
    appoint_handler, get_candidacy_handler, list_candidacies_handler, nominate_handler, vote_handler,
};
use chat::{gossip_chat_message_handler, list_messages_handler, send_message_handler};
use initiative::{
    get_initiative_handler, list_initiatives_handler, propose_handler as propose_initiative_handler,
    vote_handler as vote_initiative_handler,
};
use referendum::{
    announce_handler, get_referendum_handler, list_referendums_handler,
    vote_handler as vote_referendum_handler,
};
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
    sync_test_passwords(&pool)
        .await
        .expect("Failed to sync test citizen passwords");
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

    if node_id != "QZ-NODE" {
        if let Err(e) = sqlx::query(
            "UPDATE nodes SET status = 'dead' WHERE id = 'QZ-NODE' AND url LIKE '%localhost%'",
        )
        .execute(&pool)
        .await
        {
            tracing::warn!(error = %e, "failed to retire legacy QZ-NODE registry entry");
        }
    }

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

    let public_routes = Router::new()
        .route("/auth/login", post(login_handler))
        .route("/auth/check", get(check_password_handler))
        .route("/candidacy/list", get(list_candidacies_handler))
        .route("/candidacy/:id", get(get_candidacy_handler))
        .route("/initiative/list", get(list_initiatives_handler))
        .route("/initiative/:id", get(get_initiative_handler))
        .route("/referendum/list", get(list_referendums_handler))
        .route("/referendum/:id", get(get_referendum_handler))
        .with_state(state.clone());

    let protected_routes = Router::new()
        .route("/blocks", get(handlers::get_blocks))
        .route("/events", get(handlers::get_events))
        .route("/events/gossip", post(handlers::gossip_event))
        .route("/chat/gossip", post(gossip_chat_message_handler))
        .route("/event", post(handlers::add_event))
        .route("/nodes", get(handlers::get_nodes))
        .route("/peers", post(handlers::add_peer))
        .route("/peers/network", post(handlers::add_peer_to_network))
        .route("/online", post(handlers::online_handler))
        .route("/offline", post(handlers::offline_handler))
        .route("/citizens/online", get(handlers::list_online_citizens))
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
        .route("/svod", get(svod::get_catalog_handler))
        .route("/svod/categories", get(svod::get_categories_handler))
        .route("/svod/service/:code", get(svod::get_service_handler))
        .route("/svod/admin/service", post(svod::create_service_handler))
        .route("/svod/admin/service/:code", put(svod::update_service_handler))
        .route("/svod/admin/service/:code", delete(svod::disable_service_handler))
        .route("/auth/set-password", post(set_password_handler))
        .route("/keys", post(keys::create_api_key))
        .route("/keys", get(keys::list_api_keys))
        .route("/keys/revoke", post(keys::revoke_api_key))
        .route("/keys/sync", post(keys::sync_api_keys))
        .route("/keys/internal/export", get(keys::internal_export_keys))
        .route("/keys/internal/upsert", post(keys::internal_upsert_key))
        .route("/keys/internal/revoke", post(keys::internal_revoke_key))
        .route("/citizen/register", post(register_citizen))
        .route("/citizen/list", get(list_citizens))
        .route("/citizen/:id", get(get_citizen))
        .route("/citizen/:id/status", patch(update_status))
        .route("/citizen/:id/role", patch(update_role))
        .route("/citizen/:id/passport", post(issue_passport))
        .route("/citizen/:id/passport/revoke", post(revoke_passport))
        .route("/citizen/search", get(search_citizens))
        .route("/candidacy/nominate", post(nominate_handler))
        .route("/candidacy/:id/vote", post(vote_handler))
        .route("/candidacy/:id/appoint", post(appoint_handler))
        .route("/initiative/propose", post(propose_initiative_handler))
        .route("/initiative/:id/vote", post(vote_initiative_handler))
        .route("/referendum/announce", post(announce_handler))
        .route("/referendum/:id/vote", post(vote_referendum_handler))
        .route("/chat/messages", get(list_messages_handler))
        .route("/chat/send", post(send_message_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .route_layer(middleware::from_fn(rate_limit_middleware))
        .with_state(state.clone());

    let app = Router::new()
        .route("/status", get(handlers::status))
        .merge(public_routes)
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
