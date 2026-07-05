use serde_json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

use crate::models::{Block, Event};

fn compute_hash(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn compute_event_hash(event: &Event) -> String {
    let content = format!(
        "{}{}{}{}{}{}{}{}{}",
        event.event_id,
        event.timestamp,
        event.event_type,
        event.title,
        event.description,
        event.initiator,
        event.previous_hash,
        serde_json::to_string(&event.data).unwrap_or_default(),
        event.public
    );
    compute_hash(&content)
}

fn resolved_event_hash(event: &Event) -> String {
    event
        .hash
        .as_ref()
        .filter(|h| !h.is_empty())
        .cloned()
        .unwrap_or_else(|| compute_event_hash(event))
}

fn events_root(events: &[Event]) -> String {
    let joined: String = events.iter().map(resolved_event_hash).collect();
    compute_hash(&joined)
}

pub fn build_citizen_added_event(
    event_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    public_key: &str,
    birth_place: &str,
    role: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "CitizenAdded".to_string(),
        title: format!("Регистрация гражданина {}", citizen_name),
        description: format!("Добавлен гражданин {} с ролью {}", citizen_name, role),
        initiator: initiator.to_string(),
        data: serde_json::json!({
            "citizen_id": citizen_id,
            "citizen_name": citizen_name,
            "public_key": public_key,
            "birth_place": birth_place,
            "role": role,
        }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

pub fn build_passport_issued_event(
    event_id: &str,
    passport_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    issued_at: i64,
    expires_at: i64,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "PassportIssued".to_string(),
        title: format!("Выдача паспорта гражданину {}", citizen_name),
        description: format!("Паспорт {} выдан гражданину {}", passport_id, citizen_name),
        initiator: initiator.to_string(),
        data: serde_json::json!({
            "passport_id": passport_id,
            "citizen_id": citizen_id,
            "issued_at": issued_at,
            "expires_at": expires_at,
        }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

pub fn build_passport_revoked_event(
    event_id: &str,
    passport_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "PassportRevoked".to_string(),
        title: format!("Аннулирование паспорта {}", citizen_name),
        description: format!("Паспорт {} гражданина {} аннулирован", passport_id, citizen_name),
        initiator: initiator.to_string(),
        data: serde_json::json!({
            "passport_id": passport_id,
            "citizen_id": citizen_id,
        }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

pub fn build_citizen_suspended_event(
    event_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "CitizenSuspended".to_string(),
        title: format!("Приостановка гражданина {}", citizen_name),
        description: format!("Статус гражданина {} изменён на suspended", citizen_name),
        initiator: initiator.to_string(),
        data: serde_json::json!({ "citizen_id": citizen_id }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

pub fn build_citizen_restored_event(
    event_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "CitizenRestored".to_string(),
        title: format!("Восстановление гражданина {}", citizen_name),
        description: format!("Статус гражданина {} изменён на active", citizen_name),
        initiator: initiator.to_string(),
        data: serde_json::json!({ "citizen_id": citizen_id }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

pub fn build_citizen_updated_event(
    event_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    status: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    Event {
        event_id: event_id.to_string(),
        timestamp,
        event_type: "CitizenUpdated".to_string(),
        title: format!("Обновление статуса гражданина {}", citizen_name),
        description: format!("Статус гражданина {} изменён на {}", citizen_name, status),
        initiator: initiator.to_string(),
        data: serde_json::json!({
            "citizen_id": citizen_id,
            "status": status,
        }),
        previous_hash: "0".to_string(),
        signatures: vec![],
        hash: None,
        public: true,
    }
}

fn signed_pending_event(mut event: Event) -> Event {
    event.hash = Some(compute_event_hash(&event));
    event.signatures = vec![crate::auth::internal_node_signature(
        &event.event_id,
        event.hash.as_deref().unwrap_or(""),
    )];
    event
}

pub fn build_signed_passport_issued_event(
    event_id: &str,
    passport_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    issued_at: i64,
    expires_at: i64,
    initiator: &str,
    timestamp: i64,
) -> Event {
    signed_pending_event(build_passport_issued_event(
        event_id,
        passport_id,
        citizen_id,
        citizen_name,
        issued_at,
        expires_at,
        initiator,
        timestamp,
    ))
}

pub fn build_signed_passport_revoked_event(
    event_id: &str,
    passport_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    signed_pending_event(build_passport_revoked_event(
        event_id,
        passport_id,
        citizen_id,
        citizen_name,
        initiator,
        timestamp,
    ))
}

pub fn build_signed_citizen_status_event(
    event_id: &str,
    citizen_id: &str,
    citizen_name: &str,
    status: &str,
    initiator: &str,
    timestamp: i64,
) -> Event {
    let event = match status {
        "active" => build_citizen_restored_event(
            event_id, citizen_id, citizen_name, initiator, timestamp,
        ),
        "suspended" => build_citizen_suspended_event(
            event_id, citizen_id, citizen_name, initiator, timestamp,
        ),
        other => build_citizen_updated_event(
            event_id, citizen_id, citizen_name, other, initiator, timestamp,
        ),
    };
    signed_pending_event(event)
}

pub fn compute_block_hash(
    block_number: u64,
    timestamp: i64,
    events_count: usize,
    previous_hash: &str,
    events: &[Event],
) -> String {
    let root = events_root(events);
    compute_hash(&format!(
        "{}{}{}{}{}",
        block_number, timestamp, events_count, previous_hash, root
    ))
}

fn validate_event_hashes(events: &[Event]) -> Result<(), String> {
    for event in events {
        if let Some(stored) = event.hash.as_ref().filter(|h| !h.is_empty()) {
            let expected = compute_event_hash(event);
            if stored != &expected {
                return Err(format!(
                    "event {}: invalid hash (expected {}, got {})",
                    event.event_id, expected, stored
                ));
            }
        }
    }
    Ok(())
}

pub enum SyncBlockAction {
    Skip,
    Reject(String),
    Insert,
}

pub async fn classify_block(block: &Block, pool: &PgPool) -> SyncBlockAction {
    if block.events.len() != block.events_count {
        return SyncBlockAction::Reject(format!(
            "block #{}: events_count ({}) does not match events.len() ({})",
            block.block_number,
            block.events_count,
            block.events.len()
        ));
    }

    if let Err(reason) = validate_event_hashes(&block.events) {
        return SyncBlockAction::Reject(format!("block #{}: {}", block.block_number, reason));
    }

    let expected_hash = compute_block_hash(
        block.block_number,
        block.timestamp,
        block.events_count,
        &block.previous_hash,
        &block.events,
    );
    if block.block_hash != expected_hash {
        return SyncBlockAction::Reject(format!(
            "block #{}: invalid block_hash (expected {}, got {})",
            block.block_number, expected_hash, block.block_hash
        ));
    }

    let stored_hash: Option<String> = sqlx::query_scalar(
        "SELECT block_hash FROM blocks WHERE block_number = $1",
    )
    .bind(block.block_number as i64)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    if let Some(hash) = stored_hash {
        if hash == block.block_hash {
            return SyncBlockAction::Skip;
        }
        return SyncBlockAction::Reject(format!(
            "block #{}: already exists with a different hash",
            block.block_number
        ));
    }

    let local_tip: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(block_number), 0) FROM blocks")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    if block.block_number as i64 != local_tip + 1 {
        return SyncBlockAction::Reject(format!(
            "block #{}: expected next block after #{}, or duplicate",
            block.block_number, local_tip
        ));
    }

    let expected_previous = if local_tip == 0 {
        "0".to_string()
    } else {
        sqlx::query_scalar("SELECT block_hash FROM blocks WHERE block_number = $1")
            .bind(local_tip)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "0".to_string())
    };

    if block.previous_hash != expected_previous {
        return SyncBlockAction::Reject(format!(
            "block #{}: previous_hash mismatch (expected {}, got {})",
            block.block_number, expected_previous, block.previous_hash
        ));
    }

    SyncBlockAction::Insert
}

pub async fn insert_synced_block(pool: &PgPool, block: &Block) -> Result<(), String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("Failed to start sync transaction: {}", e))?;

    for event in &block.events {
        insert_event(&mut tx, event).await?;
    }

    crate::projection::apply_event_projections_in_tx(&mut tx, &block.events).await?;

    let block_json =
        serde_json::to_string(block).map_err(|e| format!("Failed to serialize block: {}", e))?;

    sqlx::query(
        "INSERT INTO blocks (block_number, block_hash, previous_hash, timestamp, block_data, events_count, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())",
    )
    .bind(block.block_number as i64)
    .bind(&block.block_hash)
    .bind(&block.previous_hash)
    .bind(block.timestamp)
    .bind(block_json)
    .bind(block.events_count as i32)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("Failed to insert block #{}: {}", block.block_number, e))?;

    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit sync transaction: {}", e))?;

    let event_ids: Vec<String> = block.events.iter().map(|e| e.event_id.clone()).collect();
    if let Err(e) = crate::pending::delete_by_ids(pool, &event_ids).await {
        tracing::warn!(error = %e, "failed to cleanup pending after sync");
    }

    Ok(())
}

async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let data_json =
        serde_json::to_string(&event.data).map_err(|e| format!("Failed to serialize event data: {}", e))?;
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
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("Failed to insert event {}: {}", event.event_id, e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Event;

    fn sample_event(id: &str, name: &str) -> Event {
        build_citizen_added_event(
            id,
            "citizen-uuid-1",
            name,
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986",
            "Quazar",
            "Citizen",
            "initiator",
            1_700_000_000,
        )
    }

    #[test]
    fn compute_event_hash_is_deterministic() {
        let event = sample_event("evt-1", "alice");
        assert_eq!(compute_event_hash(&event), compute_event_hash(&event));
    }

    #[test]
    fn compute_event_hash_changes_with_content() {
        let mut event = sample_event("evt-1", "alice");
        let hash1 = compute_event_hash(&event);
        event.title = "changed".to_string();
        assert_ne!(hash1, compute_event_hash(&event));
    }

    #[test]
    fn compute_block_hash_includes_events() {
        let events = vec![sample_event("evt-1", "alice"), sample_event("evt-2", "bob")];
        let hash1 = compute_block_hash(1, 100, events.len(), "0", &events);
        let mut events2 = events.clone();
        events2[0].title = "tampered".to_string();
        let hash2 = compute_block_hash(1, 100, events2.len(), "0", &events2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn validate_event_hashes_accepts_matching_hash() {
        let mut event = sample_event("evt-1", "alice");
        event.hash = Some(compute_event_hash(&event));
        assert!(validate_event_hashes(&[event]).is_ok());
    }

    #[test]
    fn validate_event_hashes_rejects_mismatch() {
        let mut event = sample_event("evt-1", "alice");
        event.hash = Some("deadbeef".to_string());
        let err = validate_event_hashes(&[event]).unwrap_err();
        assert!(err.contains("invalid hash"));
    }
}
