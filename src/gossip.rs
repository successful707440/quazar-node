use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;

use crate::auth::node_secret;
use crate::models::Event;
use crate::nodes::NodeStatus;
use crate::pending;
use crate::response::ApiResponse;
use crate::validator::EventValidator;
use crate::AppState;

pub async fn push_pending_to_peers(state: &Arc<AppState>, event: &Event) {
    let peers = match state.node_registry.get_all_nodes().await {
        Ok(peers) => peers,
        Err(e) => {
            tracing::warn!(error = %e, "gossip: failed to load peers");
            return;
        }
    };

    let client = Client::new();
    let my_id = state.node_id.clone();

    for peer in peers {
        if peer.id == my_id || peer.status != NodeStatus::Alive {
            continue;
        }
        let url = format!("{}/events/gossip", peer.url.trim_end_matches('/'));
        let event = event.clone();
        let client = client.clone();
        tokio::spawn(async move {
            match client
                .post(&url)
                .header("Authorization", format!("Bearer {}", node_secret()))
                .json(&event)
                .timeout(Duration::from_secs(3))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    tracing::debug!(peer_url = %url, event_id = %event.event_id, "gossip push ok");
                }
                Ok(resp) => {
                    tracing::warn!(
                        peer_url = %url,
                        event_id = %event.event_id,
                        status = %resp.status(),
                        "gossip push rejected"
                    );
                }
                Err(e) => {
                    tracing::warn!(peer_url = %url, event_id = %event.event_id, error = %e, "gossip push failed");
                }
            }
        });
    }
}

pub async fn receive_gossip_event(
    state: &Arc<AppState>,
    event: Event,
) -> Result<ApiResponse, String> {
    if pending::exists(&state.db, &event.event_id)
        .await
        .unwrap_or(false)
    {
        return Ok(ApiResponse::success(serde_json::json!({
            "event_id": event.event_id,
            "message": "Already in pending, skipped"
        })));
    }

    let confirmed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)",
    )
    .bind(&event.event_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(false);

    if confirmed {
        return Ok(ApiResponse::success(serde_json::json!({
            "event_id": event.event_id,
            "message": "Already confirmed, skipped"
        })));
    }

    if let Err(e) = EventValidator::validate_event(&event, &state.db).await {
        return Err(e.message());
    }

    pending::insert(&state.db, &event)
        .await
        .map_err(|e| format!("Failed to store gossip event: {}", e))?;

    Ok(ApiResponse::success(serde_json::json!({
        "event_id": event.event_id,
        "message": "Gossip event accepted"
    })))
}
