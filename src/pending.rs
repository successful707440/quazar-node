use sqlx::{PgPool, Postgres, Transaction};

use crate::models::Event;

pub async fn insert(pool: &PgPool, event: &Event) -> Result<(), String> {
    let data = serde_json::to_string(event).map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO pending_events (event_id, event_data) VALUES ($1, $2)")
        .bind(&event.event_id)
        .bind(data)
        .execute(pool)
        .await
        .map_err(|e| format!("Failed to insert pending event {}: {}", event.event_id, e))?;
    Ok(())
}

pub async fn fetch_all(pool: &PgPool) -> Result<Vec<Event>, String> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT event_data FROM pending_events ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;

    Ok(rows
        .iter()
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect())
}

pub async fn exists(pool: &PgPool, event_id: &str) -> Result<bool, String> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_events WHERE event_id = $1)")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())
}

pub async fn count(pool: &PgPool) -> Result<i64, String> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pending_events")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())
}

pub async fn delete_by_ids(pool: &PgPool, event_ids: &[String]) -> Result<(), String> {
    for event_id in event_ids {
        sqlx::query("DELETE FROM pending_events WHERE event_id = $1")
            .bind(event_id)
            .execute(pool)
            .await
            .map_err(|e| format!("Failed to delete pending event {}: {}", event_id, e))?;
    }
    Ok(())
}

pub async fn delete_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    event_ids: &[String],
) -> Result<(), String> {
    for event_id in event_ids {
        sqlx::query("DELETE FROM pending_events WHERE event_id = $1")
            .bind(event_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| format!("Failed to delete pending event {}: {}", event_id, e))?;
    }
    Ok(())
}
