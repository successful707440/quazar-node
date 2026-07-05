use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Extension, State},
    response::{IntoResponse, Json},
};
use chrono::Utc;
use reqwest::Client;
use tokio::time;

use crate::auth::{internal_node_signature, node_secret, AuthContext};
use crate::block_producer;
use crate::blockchain::{self, compute_event_hash};
use crate::gossip;
use crate::models::{AddEventRequest, Block, Event};
use crate::nodes::Node;
use crate::pending;
use crate::response::{self, ApiResponse};
use crate::validator::EventValidator;
use crate::AppState;

pub async fn add_event(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddEventRequest>,
) -> impl IntoResponse {
    if auth.is_node {
        return response::forbidden(
            "Node credentials cannot submit events via POST /event; use POST /events/gossip",
        );
    }

    let mut event = body.into_event();
    if event.initiator.trim().is_empty() {
        event.initiator = auth.citizen_name.clone();
    }

    if let Err(e) = EventValidator::validate_event(&event, &state.db).await {
        tracing::warn!(error = %e.message(), "event validation failed");
        return response::bad_request(e.message());
    }

    let hash = compute_event_hash(&event);
    if let Some(provided) = event.hash.as_ref().filter(|h| !h.is_empty()) {
        if provided != &hash {
            return response::bad_request("Event hash does not match content");
        }
    }
    event.hash = Some(hash);

    match pending::insert(&state.db, &event).await {
        Ok(()) => {
            let count = pending::count(&state.db).await.unwrap_or(0);
            tracing::info!(event_id = %event.event_id, pending = count, "event added");
            let state_gossip = state.clone();
            let event_gossip = event.clone();
            tokio::spawn(async move {
                gossip::push_pending_to_peers(&state_gossip, &event_gossip).await;
            });
            let state_clone = state.clone();
            tokio::spawn(async move {
                block_producer::try_create_block(state_clone).await;
            });
            Json(ApiResponse::success(serde_json::json!({
                "event_id": event.event_id,
                "pending_count": count,
                "message": format!("Event added ({} events waiting)", count),
            })))
            .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to add pending event");
            response::internal_error(e)
        }
    }
}

pub async fn get_events(State(state): State<Arc<AppState>>) -> Json<ApiResponse> {
    let events = pending::fetch_all(&state.db).await.unwrap_or_default();
    tracing::debug!(count = events.len(), "returning pending events");
    Json(ApiResponse::success(events))
}

pub async fn get_blocks(State(state): State<Arc<AppState>>) -> Json<ApiResponse> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT block_data FROM blocks ORDER BY block_number ASC",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let blocks: Vec<Block> = rows
        .iter()
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect();
    tracing::debug!(count = blocks.len(), "returning blocks");
    Json(ApiResponse::success(blocks))
}

pub async fn gossip_event(
    State(state): State<Arc<AppState>>,
    Json(event): Json<Event>,
) -> impl IntoResponse {
    match gossip::receive_gossip_event(&state, event).await {
        Ok(resp) => Json(resp).into_response(),
        Err(msg) => response::bad_request(msg),
    }
}

pub async fn status(State(state): State<Arc<AppState>>) -> Json<ApiResponse> {
    let pending_count = pending::count(&state.db).await.unwrap_or(0);
    let blocks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blocks")
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    let next_block: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(block_number), 0) + 1 FROM blocks",
    )
    .fetch_one(&state.db)
    .await
    .unwrap_or(1);
    let policy = block_producer::BlockPolicy::from_env();
    let nodes = state
        .node_registry
        .get_all_nodes()
        .await
        .unwrap_or_default();
    let (effective_producer, designated_producer, producer_fallback) =
        block_producer::resolve_block_producer(&state, next_block as u64, &nodes).await;

    Json(ApiResponse::success(serde_json::json!({
        "version": "0.7.0",
        "blockchain": true,
        "node_id": state.node_id,
        "pending_events_local": pending_count,
        "blocks": blocks,
        "next_block": next_block,
        "block_producer": designated_producer,
        "effective_block_producer": effective_producer,
        "block_producer_fallback": producer_fallback,
        "is_block_producer": effective_producer == state.node_id,
        "block_policy": {
            "min_events": policy.min_events,
            "max_wait_secs": policy.max_wait_secs,
            "sync_interval_secs": policy.sync_interval_secs,
        }
    })))
}

pub async fn get_nodes(State(state): State<Arc<AppState>>) -> Json<ApiResponse> {
    let nodes = state.node_registry.get_all_nodes().await.unwrap_or_default();
    Json(ApiResponse::success(nodes))
}

pub async fn add_peer(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(peer): Json<Node>,
) -> impl IntoResponse {
    if !auth.can_manage_peers() {
        return response::forbidden("Only Aiya and Guardian can add peers");
    }

    match state.node_registry.upsert_node(&peer).await {
        Ok(_) => Json(ApiResponse::success(serde_json::json!({
            "message": "Peer added"
        })))
        .into_response(),
        Err(e) => response::internal_error(format!("Failed to add peer: {}", e)),
    }
}

pub async fn online_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let citizen_id = payload
        .get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if citizen_id.is_empty() {
        return response::forbidden("citizen_id is required");
    }
    if !auth.authorize_citizen_ref(&state.db, citizen_id).await {
        return response::forbidden("Cannot change online status for another citizen");
    }

    let now = Utc::now().timestamp();
    let _ = sqlx::query(
        "INSERT INTO citizen_status (citizen_id, status, last_seen) VALUES ($1, 'online', $2)
         ON CONFLICT (citizen_id) DO UPDATE SET status = 'online', last_seen = EXCLUDED.last_seen",
    )
    .bind(citizen_id)
    .bind(now)
    .execute(&state.db)
    .await;

    Json(ApiResponse::success(serde_json::json!({
        "citizen_id": citizen_id,
        "message": "Citizen marked as online"
    })))
    .into_response()
}

pub async fn offline_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let citizen_id = payload
        .get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if citizen_id.is_empty() {
        return response::forbidden("citizen_id is required");
    }
    if !auth.authorize_citizen_ref(&state.db, citizen_id).await {
        return response::forbidden("Cannot change offline status for another citizen");
    }

    let now = Utc::now().timestamp();
    let _ = sqlx::query(
        "UPDATE citizen_status SET status = 'offline', last_seen = $1 WHERE citizen_id = $2",
    )
    .bind(now)
    .bind(citizen_id)
    .execute(&state.db)
    .await;

    Json(ApiResponse::success(serde_json::json!({
        "citizen_id": citizen_id,
        "message": "Citizen marked as offline"
    })))
    .into_response()
}

pub async fn cast_vote_handler(
    Extension(auth): Extension<AuthContext>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let vote_id = payload.get("vote_id").and_then(|v| v.as_str()).unwrap_or("");
    let citizen_id = payload
        .get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let choice = payload.get("choice").and_then(|v| v.as_str()).unwrap_or("");

    if vote_id.is_empty() {
        return response::bad_request("vote_id is required");
    }
    if choice.is_empty() {
        return response::bad_request("choice is required");
    }
    if citizen_id.is_empty() {
        return response::forbidden("citizen_id is required");
    }
    if !auth.authorize_citizen_ref(&state.db, citizen_id).await {
        return response::forbidden("Cannot cast a vote on behalf of another citizen");
    }

    let vote_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM votes WHERE vote_id = $1)",
    )
    .bind(vote_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(false);

    if !vote_exists {
        return response::bad_request("Vote not found");
    }

    match crate::votes::vote_is_active(&state.db, vote_id).await {
        Ok(true) => {}
        Ok(false) => return response::bad_request("Vote is not active"),
        Err(e) => return response::internal_error(format!("Failed to check vote status: {}", e)),
    }

    let now = Utc::now().timestamp();
    let _ = sqlx::query(
        "INSERT INTO vote_choices (vote_id, citizen_id, choice, voted_at) VALUES ($1, $2, $3, $4)
         ON CONFLICT (vote_id, citizen_id) DO UPDATE SET choice = EXCLUDED.choice, voted_at = EXCLUDED.voted_at",
    )
    .bind(vote_id)
    .bind(citizen_id)
    .bind(choice)
    .bind(now)
    .execute(&state.db)
    .await;

    Json(ApiResponse::success(serde_json::json!({
        "message": format!("Vote cast for {}", choice)
    })))
    .into_response()
}

pub async fn add_peer_to_network(
    State(state): State<Arc<AppState>>,
    Json(peer): Json<Node>,
) -> Json<ApiResponse> {
    tracing::info!(peer_id = %peer.id, url = %peer.url, "peer network add requested");

    let mut event = Event {
        event_id: format!("peer_add_{}", Utc::now().timestamp()),
        timestamp: Utc::now().timestamp(),
        event_type: "PeerListUpdate".to_string(),
        title: "Добавление нового узла".to_string(),
        description: format!("Добавлен узел {} ({})", peer.id, peer.url),
        initiator: state.node_id.clone(),
        data: serde_json::json!({
            "peers": [{
                "id": peer.id,
                "url": peer.url,
                "status": peer.status.to_string(),
                "version": peer.version,
                "last_seen": peer.last_seen.to_rfc3339(),
            }]
        }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    };
    event.hash = Some(compute_event_hash(&event));
    event.signatures = vec![internal_node_signature(
        &event.event_id,
        event.hash.as_deref().unwrap_or(""),
    )];

    if let Err(e) = EventValidator::validate_event(&event, &state.db).await {
        tracing::warn!(error = %e.message(), "peer event validation failed");
        return Json(ApiResponse::error(e.message()));
    }

    match pending::insert(&state.db, &event).await {
        Ok(()) => {
            tracing::info!(event_id = %event.event_id, "peer event added to pending");
            let state_gossip = state.clone();
            let event_gossip = event.clone();
            tokio::spawn(async move {
                gossip::push_pending_to_peers(&state_gossip, &event_gossip).await;
            });
            let state_clone = state.clone();
            tokio::spawn(async move {
                block_producer::try_create_block(state_clone).await;
            });
            Json(ApiResponse::success(serde_json::json!({
                "message": "Peer will be added to network via blockchain"
            })))
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to add peer event");
            Json(ApiResponse::error(e))
        }
    }
}

pub async fn background_sync(state: Arc<AppState>) {
    let policy = block_producer::BlockPolicy::from_env();
    let mut interval = time::interval(Duration::from_secs(policy.sync_interval_secs));
    let client = Client::new();

    loop {
        interval.tick().await;
        tracing::debug!("background sync tick");

        tracing::debug!(node_id = %state.node_id, "background sync: attempting block production");
        block_producer::try_create_block(state.clone()).await;

        let peers = state.node_registry.get_all_nodes().await.unwrap_or_default();
        let my_id = state.node_id.clone();
        let pool = state.db.clone();

        for peer in peers {
            if peer.id == my_id {
                continue;
            }

            tracing::debug!(peer_id = %peer.id, url = %peer.url, "fetching blocks from peer");
            match client
                .get(format!("{}/blocks", peer.url))
                .header("Authorization", format!("Bearer {}", node_secret()))
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(response) => {
                    if let Ok(body) = response.json::<ApiResponse>().await {
                        if let Some(mut blocks) = response::decode_data::<Vec<Block>>(&body) {
                            blocks.sort_by_key(|b| b.block_number);
                            for block in blocks {
                                match blockchain::classify_block(&block, &pool).await {
                                    blockchain::SyncBlockAction::Skip => {
                                        tracing::debug!(
                                            block = block.block_number,
                                            peer = %peer.id,
                                            "block already synced"
                                        );
                                    }
                                    blockchain::SyncBlockAction::Reject(reason) => {
                                        tracing::warn!(
                                            block = block.block_number,
                                            peer = %peer.id,
                                            reason = %reason,
                                            "block rejected"
                                        );
                                    }
                                    blockchain::SyncBlockAction::Insert => {
                                        match blockchain::insert_synced_block(&pool, &block).await {
                                            Ok(()) => tracing::info!(
                                                block = block.block_number,
                                                peer = %peer.id,
                                                "block synced"
                                            ),
                                            Err(e) => tracing::error!(
                                                block = block.block_number,
                                                peer = %peer.id,
                                                error = %e,
                                                "failed to insert synced block"
                                            ),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(peer = %peer.url, error = %e, "failed to connect to peer"),
            }
        }
    }
}

#[cfg(test)]
mod add_event_tests {
    use crate::models::AddEventRequest;

    fn sample_event_json() -> &'static str {
        r#"{
            "event_id": "test_evt_1",
            "timestamp": 1700000000,
            "event_type": "VoteCast",
            "title": "Test vote",
            "description": "Test",
            "initiator": "alice",
            "data": {"vote_id": "v1", "citizen_id": "c1", "choice": "yes"},
            "previous_hash": "0",
            "signatures": ["reg_sig_placeholder"],
            "hash": null,
            "public": true
        }"#
    }

    #[test]
    fn add_event_request_parses_flat_body_without_key() {
        let req: AddEventRequest = serde_json::from_str(sample_event_json()).unwrap();
        let event = req.into_event();
        assert_eq!(event.event_id, "test_evt_1");
    }

    #[test]
    fn add_event_request_ignores_legacy_key_in_body() {
        let json = r#"{
            "key": "legacy-api-key-in-body",
            "event_id": "test_evt_2",
            "timestamp": 1700000000,
            "event_type": "VoteCast",
            "title": "Test",
            "description": "Test",
            "initiator": "alice",
            "data": {},
            "previous_hash": "0",
            "signatures": [],
            "hash": null,
            "public": true
        }"#;
        let req: AddEventRequest = serde_json::from_str(json).unwrap();
        let event = req.into_event();
        assert_eq!(event.event_id, "test_evt_2");
    }

    #[test]
    fn add_event_request_parses_wrapped_event() {
        let json = format!(r#"{{"event": {}}}"#, sample_event_json());
        let req: AddEventRequest = serde_json::from_str(&json).unwrap();
        let event = req.into_event();
        assert_eq!(event.event_id, "test_evt_1");
    }
}
