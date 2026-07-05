use sqlx::{Postgres, Transaction};

use crate::models::Event;
use crate::types::Role;

pub async fn apply_event_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    events: &[Event],
) -> Result<(), String> {
    for event in events {
        match event.event_type.as_str() {
            "CitizenAdded" => apply_citizen_added(tx, event).await?,
            "PassportIssued" => apply_passport_issued(tx, event).await?,
            "PassportRevoked" => apply_passport_revoked(tx, event).await?,
            "CitizenSuspended" => apply_citizen_status(tx, event, "suspended").await?,
            "CitizenRestored" => apply_citizen_status(tx, event, "active").await?,
            "CitizenUpdated" => apply_citizen_updated(tx, event).await?,
            _ => {}
        }
    }
    Ok(())
}

fn require_str<'a>(data: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    data.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{}: missing {}", field, field))
}

fn require_i64(data: &serde_json::Value, field: &str) -> Result<i64, String> {
    data.get(field)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("{}: missing {}", field, field))
}

async fn apply_citizen_added(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let citizen_name = require_str(&event.data, "citizen_name")?;
    let public_key = require_str(&event.data, "public_key")?;
    let role_str = event
        .data
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or("Citizen");
    let role = Role::from_str(role_str)
        .ok_or_else(|| format!("CitizenAdded: invalid role {}", role_str))?;

    sqlx::query(
        r#"
        INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
        VALUES ($1, $2, $3, 'active', $4, $5, FALSE)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            public_key = EXCLUDED.public_key,
            role = EXCLUDED.role
        "#,
    )
    .bind(citizen_id)
    .bind(citizen_name)
    .bind(public_key)
    .bind(role.as_str())
    .bind(event.timestamp)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CitizenAdded projection failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        name = %citizen_name,
        event_id = %event.event_id,
        "CitizenAdded projected to SQL"
    );

    Ok(())
}

async fn apply_passport_issued(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let passport_id = require_str(&event.data, "passport_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let issued_at = event
        .data
        .get("issued_at")
        .and_then(|v| v.as_i64())
        .unwrap_or(event.timestamp);
    let expires_at = require_i64(&event.data, "expires_at")?;

    sqlx::query(
        "INSERT INTO passports (id, citizen_id, issued_at, expires_at, is_valid)
         VALUES ($1, $2, $3, $4, TRUE)
         ON CONFLICT (id) DO UPDATE SET
            expires_at = EXCLUDED.expires_at,
            is_valid = TRUE",
    )
    .bind(passport_id)
    .bind(citizen_id)
    .bind(issued_at)
    .bind(expires_at)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportIssued projection failed for {}: {}", passport_id, e))?;

    sqlx::query(
        "UPDATE citizens SET passport_issued = TRUE, passport_expires = $1 WHERE id = $2",
    )
    .bind(expires_at)
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportIssued citizen update failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        passport_id = %passport_id,
        event_id = %event.event_id,
        "PassportIssued projected to SQL"
    );

    Ok(())
}

async fn apply_passport_revoked(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let passport_id = require_str(&event.data, "passport_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;

    sqlx::query("UPDATE passports SET is_valid = FALSE WHERE id = $1")
        .bind(passport_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("PassportRevoked projection failed for {}: {}", passport_id, e))?;

    sqlx::query(
        "UPDATE citizens SET passport_issued = FALSE, passport_expires = NULL WHERE id = $1",
    )
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportRevoked citizen update failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        passport_id = %passport_id,
        event_id = %event.event_id,
        "PassportRevoked projected to SQL"
    );

    Ok(())
}

async fn apply_citizen_status(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    status: &str,
) -> Result<(), String> {
    let citizen_id = require_str(&event.data, "citizen_id")?;

    sqlx::query("UPDATE citizens SET status = $1 WHERE id = $2")
        .bind(status)
        .bind(citizen_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("{} projection failed for {}: {}", event.event_type, citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        status = %status,
        event_id = %event.event_id,
        "citizen status projected to SQL"
    );

    Ok(())
}

async fn apply_citizen_updated(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let status = require_str(&event.data, "status")?;
    apply_citizen_status(tx, event, status).await
}
