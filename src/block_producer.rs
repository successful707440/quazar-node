use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use sqlx::PgPool;

use crate::auth::node_secret;
use crate::blockchain::{self, compute_block_hash, SyncBlockAction};
use crate::nodes::{Node, NodeStatus};
use crate::response::{self, ApiResponse};
use crate::pending;
use crate::models::{Block, Event};
use crate::AppState;

pub struct BlockPolicy {
    pub min_events: usize,
    pub max_wait_secs: i64,
    pub sync_interval_secs: u64,
}

impl BlockPolicy {
    pub fn from_env() -> Self {
        Self {
            min_events: std::env::var("QUAZAR_BLOCK_MIN_EVENTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            max_wait_secs: std::env::var("QUAZAR_BLOCK_MAX_WAIT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            sync_interval_secs: std::env::var("QUAZAR_SYNC_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        }
    }
}

pub fn producer_id_for_block(block_number: u64, nodes: &[Node]) -> Option<String> {
    let mut alive: Vec<&Node> = nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Alive)
        // Ghost entries from default QUAZAR_NODE_ID / localhost bootstrap in old blocks.
        .filter(|n| !n.url.contains("localhost"))
        .collect();
    alive.sort_by(|a, b| a.id.cmp(&b.id));
    if alive.is_empty() {
        return None;
    }
    let idx = block_number.saturating_sub(1) as usize % alive.len();
    Some(alive[idx].id.clone())
}

/// Round-robin producer. Fallback to the current node only when no peers are alive or the
/// designated node is marked dead (single-node / stale bootstrap). A temporarily unreachable
/// but alive designated producer is never replaced — avoids competing forks.
pub fn resolve_block_producer(
    state: &AppState,
    block_number: u64,
    nodes: &[Node],
) -> (String, Option<String>, bool) {
    let designated = producer_id_for_block(block_number, nodes);

    let producer = match designated.as_deref() {
        None => {
            tracing::info!(
                block = block_number,
                node_id = %state.node_id,
                alive_nodes = 0,
                "block producer fallback: no alive nodes in registry, current node will produce"
            );
            state.node_id.clone()
        }
        Some(id)
            if !nodes
                .iter()
                .any(|n| n.id == id && n.status == NodeStatus::Alive) =>
        {
            tracing::info!(
                block = block_number,
                designated = %id,
                node_id = %state.node_id,
                "block producer fallback: designated node not alive, current node will produce"
            );
            state.node_id.clone()
        }
        Some(id) if id == state.node_id => state.node_id.clone(),
        Some(id) => id.to_string(),
    };

    let used_fallback = designated.as_deref() != Some(producer.as_str());
    (producer, designated, used_fallback)
}

pub async fn sync_blocks_from_peers(state: &Arc<AppState>) {
    let client = Client::new();
    let nodes = match state.node_registry.get_all_nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::warn!(error = %e, "block sync: failed to load peers");
            return;
        }
    };
    let my_id = state.node_id.clone();
    let pool = state.db.clone();
    for peer in &nodes {
        if peer.id == my_id || peer.status != NodeStatus::Alive {
            continue;
        }

        tracing::debug!(peer_id = %peer.id, url = %peer.url, "fetching blocks from peer");
        let response = match client
            .get(format!("{}/blocks", peer.url))
            .header("Authorization", format!("Bearer {}", node_secret()))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(peer = %peer.url, error = %e, "failed to connect to peer");
                continue;
            }
        };

        let body = match response.json::<ApiResponse>().await {
            Ok(body) => body,
            Err(e) => {
                tracing::warn!(peer = %peer.url, error = %e, "failed to parse peer blocks response");
                continue;
            }
        };

        let Some(mut blocks) = response::decode_data::<Vec<Block>>(&body) else {
            continue;
        };
        blocks.sort_by_key(|b| b.block_number);

        let local_blocks = blockchain::load_blocks_from_db(&pool).await.unwrap_or_default();
        let mut reconciled = false;
        for block in &blocks {
            match blockchain::classify_block(block, &pool).await {
                SyncBlockAction::Skip => {
                    tracing::debug!(
                        block = block.block_number,
                        peer = %peer.id,
                        "block already synced"
                    );
                }
                SyncBlockAction::Reject(reason) => {
                    if reason.contains("already exists with a different hash") {
                        if let Some(fork_block) = blockchain::block_number_from_reject(&reason) {
                            let designated = producer_id_for_block(fork_block, &nodes);
                            let we_are_designated = designated.as_deref() == Some(my_id.as_str());
                            let peer_is_designated =
                                designated.as_deref() == Some(peer.id.as_str());

                            if we_are_designated
                                && blockchain::chains_share_prefix(
                                    &local_blocks,
                                    &blocks,
                                    fork_block,
                                )
                            {
                                tracing::warn!(
                                    fork = fork_block,
                                    peer = %peer.id,
                                    "designated producer self-healing chain from peer copy"
                                );
                                match blockchain::adopt_peer_suffix(&pool, fork_block, &blocks)
                                    .await
                                {
                                    Ok(()) => {
                                        tracing::info!(
                                            fork = fork_block,
                                            peer = %peer.id,
                                            "chain self-healed from peer"
                                        );
                                        reconciled = true;
                                        break;
                                    }
                                    Err(e) => tracing::error!(
                                        fork = fork_block,
                                        peer = %peer.id,
                                        error = %e,
                                        "chain self-heal failed"
                                    ),
                                }
                            } else if peer_is_designated && !we_are_designated {
                                tracing::info!(
                                    fork = fork_block,
                                    peer = %peer.id,
                                    "fork detected; waiting for designated producer to publish canonical chain"
                                );
                            } else {
                                tracing::warn!(
                                    block = block.block_number,
                                    peer = %peer.id,
                                    reason = %reason,
                                    "block rejected at fork"
                                );
                            }
                        } else {
                            tracing::warn!(
                                block = block.block_number,
                                peer = %peer.id,
                                reason = %reason,
                                "block rejected"
                            );
                        }
                    } else {
                        tracing::warn!(
                            block = block.block_number,
                            peer = %peer.id,
                            reason = %reason,
                            "block rejected"
                        );
                    }
                }
                SyncBlockAction::Insert => {
                    match blockchain::insert_synced_block(&pool, block).await {
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

        if reconciled {
            apply_peer_list_updates_from_blocks(state, &blocks).await;
        }
    }
}

async fn apply_peer_list_updates_from_blocks(state: &AppState, blocks: &[Block]) {
    for block in blocks {
        apply_peer_list_updates(state, block).await;
    }
}

async fn confirmed_event_ids(pool: &PgPool) -> HashSet<String> {
    sqlx::query_scalar::<_, String>("SELECT event_id FROM events")
        .fetch_all(pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect()
}

async fn fetch_peer_pending(client: &Client, peer_url: &str) -> Vec<Event> {
    let url = format!("{}/events", peer_url.trim_end_matches('/'));
    match client
        .get(&url)
        .header("Authorization", format!("Bearer {}", node_secret()))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => resp
            .json::<crate::response::ApiResponse>()
            .await
            .ok()
            .and_then(|body| crate::response::decode_data(&body))
            .unwrap_or_default(),
        Ok(resp) => {
            tracing::warn!(peer_url = %peer_url, status = %resp.status(), "failed to fetch pending from peer");
            Vec::new()
        }
        Err(e) => {
            tracing::warn!(peer_url = %peer_url, error = %e, "failed to fetch pending from peer");
            Vec::new()
        }
    }
}

pub async fn collect_network_pending(state: &AppState) -> Result<Vec<Event>, String> {
    let confirmed = confirmed_event_ids(&state.db).await;
    let mut by_id: HashMap<String, Event> = HashMap::new();

    for event in pending::fetch_all(&state.db).await? {
        if !confirmed.contains(&event.event_id) {
            by_id.insert(event.event_id.clone(), event);
        }
    }

    let client = Client::new();
    let peers = state
        .node_registry
        .get_all_nodes()
        .await
        .map_err(|e| e.to_string())?;

    for peer in &peers {
        if peer.id == state.node_id || peer.status != NodeStatus::Alive {
            continue;
        }
        for event in fetch_peer_pending(&client, &peer.url).await {
            if confirmed.contains(&event.event_id) {
                continue;
            }
            if let Some(existing) = by_id.get(&event.event_id) {
                if existing.hash != event.hash {
                    tracing::warn!(
                        event_id = %event.event_id,
                        peer = %peer.id,
                        "conflicting pending event from peer, rejecting peer copy"
                    );
                }
                continue;
            }
            by_id.insert(event.event_id.clone(), event);
        }
    }

    let mut events: Vec<Event> = by_id.into_values().collect();
    events.sort_by(|a, b| (a.timestamp, &a.event_id).cmp(&(b.timestamp, &b.event_id)));
    Ok(events)
}

fn block_creation_gate(events: &[Event], policy: &BlockPolicy) -> (bool, String) {
    if events.is_empty() {
        return (false, "no pending events".to_string());
    }
    if events.len() >= policy.min_events {
        return (
            true,
            format!(
                "pending_count {} >= min_events {}",
                events.len(),
                policy.min_events
            ),
        );
    }
    let now = Utc::now().timestamp();
    let oldest = events.iter().map(|e| e.timestamp).min().unwrap_or(now);
    let age_secs = now - oldest;
    if age_secs >= policy.max_wait_secs {
        return (
            true,
            format!(
                "oldest pending age {}s >= max_wait_secs {}",
                age_secs, policy.max_wait_secs
            ),
        );
    }
    (
        false,
        format!(
            "pending_count {} < min_events {} and oldest age {}s < max_wait_secs {}",
            events.len(),
            policy.min_events,
            age_secs,
            policy.max_wait_secs
        ),
    )
}

pub async fn create_block_from_events(
    state: &AppState,
    events: Vec<Event>,
) -> Result<Block, String> {
    if events.is_empty() {
        return Err("No events to include in block".to_string());
    }

    let events_count = events.len();
    tracing::info!(events = events_count, "creating block from network pending");

    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| format!("Failed to start transaction: {}", e))?;

    sqlx::query("SELECT pg_advisory_xact_lock(42424242)")
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to acquire block lock: {}", e))?;

    let confirmed: HashSet<String> = sqlx::query_scalar("SELECT event_id FROM events")
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();

    let events: Vec<Event> = events
        .into_iter()
        .filter(|e| !confirmed.contains(&e.event_id))
        .collect();

    if events.is_empty() {
        return Err("All collected events are already confirmed".to_string());
    }

    for event in &events {
        let data_json = serde_json::to_string(&event.data)
            .map_err(|e| format!("Failed to serialize event data: {}", e))?;
        let sig_json = serde_json::to_string(&event.signatures)
            .map_err(|e| format!("Failed to serialize signatures: {}", e))?;
        sqlx::query(
            "INSERT INTO events (event_id, timestamp, event_type, title, description, initiator, data, previous_hash, signatures, hash, created_at, public)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&event.event_id)
        .bind(event.timestamp)
        .bind(&event.event_type)
        .bind(&event.title)
        .bind(&event.description)
        .bind(&event.initiator)
        .bind(data_json)
        .bind(&event.previous_hash)
        .bind(sig_json)
        .bind(event.hash.as_deref().unwrap_or(""))
        .bind(event.public)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to insert event {}: {}", event.event_id, e))?;
    }

    crate::projection::apply_event_projections_in_tx(&mut tx, &events).await?;

    let last_hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM blocks ORDER BY block_number DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| format!("Failed to read last block hash: {}", e))?
    .unwrap_or_else(|| "0".to_string());

    let block_number: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(block_number), 0) + 1 FROM blocks",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("Failed to compute next block number: {}", e))?;

    let block = Block {
        block_number: block_number as u64,
        timestamp: Utc::now().timestamp(),
        events: events.clone(),
        previous_hash: last_hash,
        block_hash: String::new(),
        events_count: events.len(),
    };

    let block_hash = compute_block_hash(
        block.block_number,
        block.timestamp,
        block.events_count,
        &block.previous_hash,
        &events,
    );

    let mut final_block = block;
    final_block.block_hash = block_hash;

    let block_json = serde_json::to_string(&final_block)
        .map_err(|e| format!("Failed to serialize block: {}", e))?;

    sqlx::query(
        "INSERT INTO blocks (block_number, block_hash, previous_hash, timestamp, block_data, events_count, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())",
    )
    .bind(final_block.block_number as i64)
    .bind(&final_block.block_hash)
    .bind(&final_block.previous_hash)
    .bind(final_block.timestamp)
    .bind(block_json)
    .bind(final_block.events_count as i32)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("Failed to insert block #{}: {}", block_number, e))?;

    let event_ids: Vec<String> = events.iter().map(|e| e.event_id.clone()).collect();
    pending::delete_in_tx(&mut tx, &event_ids).await?;

    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit block transaction: {}", e))?;

    apply_peer_list_updates(state, &final_block).await;

    tracing::info!(block = final_block.block_number, "block saved");
    Ok(final_block)
}

async fn apply_peer_list_updates(state: &AppState, block: &Block) {
    for event in &block.events {
        if event.event_type != "PeerListUpdate" {
            continue;
        }
        tracing::debug!(event_id = %event.event_id, "applying peer list update from block");
        if let Some(peers) = event.data.get("peers").and_then(|v| v.as_array()) {
            for peer_data in peers {
                if let (Some(id), Some(url), Some(status_str)) = (
                    peer_data.get("id").and_then(|v| v.as_str()),
                    peer_data.get("url").and_then(|v| v.as_str()),
                    peer_data.get("status").and_then(|v| v.as_str()),
                ) {
                    let status = match status_str {
                        "alive" => NodeStatus::Alive,
                        "dead" => NodeStatus::Dead,
                        _ => NodeStatus::Alive,
                    };
                    let peer = Node {
                        id: id.to_string(),
                        url: url.to_string(),
                        public_key: None,
                        status,
                        last_seen: Utc::now(),
                        version: "0.7.0".to_string(),
                    };
                    if let Err(e) = state.node_registry.upsert_node(&peer).await {
                        tracing::warn!(peer_id = %id, error = %e, "failed to upsert peer from block");
                    } else {
                        tracing::info!(peer_id = %id, url = %url, "peer added from block");
                    }
                }
            }
        }
    }
}

pub async fn try_create_block(state: Arc<AppState>) {
    let policy = BlockPolicy::from_env();

    let nodes = match state.node_registry.get_all_nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::error!(error = %e, "block producer: failed to load nodes");
            return;
        }
    };

    let next_block_number = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(block_number), 0) + 1 FROM blocks",
    )
    .fetch_one(&state.db)
    .await
    .unwrap_or(1);

    let alive_count = nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Alive)
        .count();
    let (producer, designated, used_fallback) =
        resolve_block_producer(&state, next_block_number as u64, &nodes);

    let force_producer = std::env::var("QUAZAR_FORCE_PRODUCER")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if !force_producer && producer != state.node_id {
        tracing::info!(
            block = next_block_number,
            designated = ?designated,
            producer = %producer,
            node_id = %state.node_id,
            alive_nodes = alive_count,
            fallback = used_fallback,
            "skipping block production on this node (another node is producer)"
        );
        return;
    }

    tracing::info!(
        block = next_block_number,
        designated = ?designated,
        node_id = %state.node_id,
        alive_nodes = alive_count,
        fallback = used_fallback,
        min_events = policy.min_events,
        max_wait_secs = policy.max_wait_secs,
        "this node is block producer, evaluating pending events"
    );

    let events = match collect_network_pending(&state).await {
        Ok(events) => events,
        Err(e) => {
            tracing::error!(error = %e, "failed to collect network pending");
            return;
        }
    };

    let (should_create, gate_reason) = block_creation_gate(&events, &policy);
    if !should_create {
        tracing::info!(
            block = next_block_number,
            pending = events.len(),
            min_events = policy.min_events,
            max_wait_secs = policy.max_wait_secs,
            reason = %gate_reason,
            "block creation threshold not met"
        );
        return;
    }

    tracing::info!(
        block = next_block_number,
        pending = events.len(),
        reason = %gate_reason,
        "block creation threshold met, creating block"
    );

    if let Err(e) = create_block_from_events(&state, events).await {
        tracing::error!(error = %e, block = next_block_number, "failed to create block");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            url: format!("http://{id}.example"),
            public_key: None,
            status: NodeStatus::Alive,
            last_seen: Utc::now(),
            version: "0.7.0".to_string(),
        }
    }

    #[test]
    fn producer_id_rotates_among_alive_nodes() {
        let nodes = vec![node("QZ-NODE-1"), node("QZ-NODE-2")];
        assert_eq!(
            producer_id_for_block(1, &nodes).as_deref(),
            Some("QZ-NODE-1")
        );
        assert_eq!(
            producer_id_for_block(2, &nodes).as_deref(),
            Some("QZ-NODE-2")
        );
        assert_eq!(
            producer_id_for_block(3, &nodes).as_deref(),
            Some("QZ-NODE-1")
        );
    }

    #[test]
    fn producer_id_none_when_no_alive_nodes() {
        let mut dead = node("QZ-NODE-1");
        dead.status = NodeStatus::Dead;
        assert!(producer_id_for_block(1, &[dead]).is_none());
    }

    #[test]
    fn block_creation_gate_respects_min_events_and_max_wait() {
        let policy = BlockPolicy {
            min_events: 3,
            max_wait_secs: 30,
            sync_interval_secs: 60,
        };
        let events: Vec<Event> = (0..2)
            .map(|i| Event {
                event_id: format!("e{i}"),
                timestamp: Utc::now().timestamp(),
                event_type: "Test".into(),
                title: String::new(),
                description: String::new(),
                initiator: String::new(),
                data: serde_json::json!({}),
                previous_hash: "0".into(),
                signatures: vec![],
                hash: None,
                public: true,
            })
            .collect();

        let (ok, reason) = block_creation_gate(&events, &policy);
        assert!(!ok, "reason: {reason}");

        let mut many = events.clone();
        many.push(events[0].clone());
        let (ok, reason) = block_creation_gate(&many, &policy);
        assert!(ok, "reason: {reason}");
    }
}