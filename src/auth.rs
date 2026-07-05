use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use sqlx::PgPool;

pub use crate::types::Role;

static MASTER_KEY: OnceLock<String> = OnceLock::new();
static NODE_SECRET: OnceLock<String> = OnceLock::new();
static REG_SECRET: OnceLock<String> = OnceLock::new();

pub fn init_master_key() {
    let key = std::env::var("QUAZAR_MASTER_KEY")
        .expect("QUAZAR_MASTER_KEY must be set (e.g. in .env)");
    MASTER_KEY.set(key).expect("init_master_key called twice");
}

pub fn init_node_secret() {
    let secret = match std::env::var("QUAZAR_NODE_SECRET") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            tracing::warn!("QUAZAR_NODE_SECRET not set — using master key for P2P (dev only)");
            master_key().to_string()
        }
    };
    NODE_SECRET
        .set(secret)
        .expect("init_node_secret called twice");
}

pub fn init_reg_secret() {
    let secret = match std::env::var("QUAZAR_REG_SECRET") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            tracing::warn!(
                "QUAZAR_REG_SECRET not set — using master-derived registration secret (dev only; must not equal QUAZAR_NODE_SECRET)"
            );
            format!("quazar_reg:{}", master_key())
        }
    };
    REG_SECRET
        .set(secret)
        .expect("init_reg_secret called twice");
}

pub fn master_key() -> &'static str {
    MASTER_KEY.get().expect("Master key not initialized")
}

pub fn node_secret() -> &'static str {
    NODE_SECRET.get().expect("Node secret not initialized")
}

pub fn reg_secret() -> &'static str {
    REG_SECRET.get().expect("Registration secret not initialized")
}

pub fn is_node_secret(key: &str) -> bool {
    key == node_secret()
}

pub fn compute_registration_signature(
    citizen_id: &str,
    public_key: &str,
    event_hash: &str,
    secret: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quazar_reg_sig:");
    hasher.update(citizen_id.as_bytes());
    hasher.update(public_key.as_bytes());
    hasher.update(event_hash.as_bytes());
    hasher.update(secret.as_bytes());
    format!("reg_sig_{:x}", hasher.finalize())
}

pub fn compute_node_signature(event_id: &str, event_hash: &str, secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quazar_node_sig:");
    hasher.update(event_id.as_bytes());
    hasher.update(event_hash.as_bytes());
    hasher.update(secret.as_bytes());
    format!("node_sig_{:x}", hasher.finalize())
}

pub fn registration_signature(citizen_id: &str, public_key: &str, event_hash: &str) -> String {
    compute_registration_signature(citizen_id, public_key, event_hash, reg_secret())
}

pub fn internal_node_signature(event_id: &str, event_hash: &str) -> String {
    compute_node_signature(event_id, event_hash, node_secret())
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiKey {
    pub key: String,
    pub role: Role,
    pub citizen_name: String,
}

#[derive(sqlx::FromRow)]
struct ApiKeyRow {
    key: String,
    role: String,
    citizen_name: String,
}

impl From<ApiKeyRow> for ApiKey {
    fn from(row: ApiKeyRow) -> Self {
        Self {
            key: row.key,
            role: Role::from_str(&row.role).unwrap_or(Role::Citizen),
            citizen_name: row.citizen_name,
        }
    }
}

pub struct KeyStore;

impl KeyStore {
    pub async fn validate_key(pool: &PgPool, key: &str) -> Option<ApiKey> {
        sqlx::query_as::<_, ApiKeyRow>(
            "SELECT key, role, citizen_name FROM api_keys WHERE key = $1 AND is_active = TRUE",
        )
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(ApiKey::from)
    }

    pub async fn upsert_key(
        pool: &PgPool,
        key: &str,
        role: Role,
        citizen_name: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO api_keys (key, role, citizen_name, is_active, revoked_at)
            VALUES ($1, $2, $3, TRUE, NULL)
            ON CONFLICT (key) DO UPDATE SET
                role = EXCLUDED.role,
                citizen_name = EXCLUDED.citizen_name,
                is_active = TRUE,
                revoked_at = NULL
            "#,
        )
        .bind(key)
        .bind(role.as_str())
        .bind(citizen_name)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn revoke_key(pool: &PgPool, key: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE api_keys SET is_active = FALSE, revoked_at = NOW() WHERE key = $1 AND is_active = TRUE",
        )
        .bind(key)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn count_active(pool: &PgPool) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE is_active = TRUE")
            .fetch_one(pool)
            .await
    }

    pub async fn list_active(pool: &PgPool) -> Result<Vec<ApiKeySummary>, sqlx::Error> {
        let rows = sqlx::query_as::<_, ApiKeyRow>(
            "SELECT key, role, citizen_name FROM api_keys WHERE is_active = TRUE ORDER BY created_at",
        )
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ApiKeySummary {
                key_masked: mask_key(&row.key),
                role: row.role,
                citizen_name: row.citizen_name,
            })
            .collect())
    }

    pub async fn sync_from_env(pool: &PgPool) -> Result<(), sqlx::Error> {
        sync_env_keys(pool).await?;
        sync_test_keys(pool).await?;
        let count = Self::count_active(pool).await.unwrap_or(0);
        tracing::info!(count, "API keys synced to PostgreSQL");
        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ApiKeySummary {
    pub key_masked: String,
    pub role: String,
    pub citizen_name: String,
}

pub fn mask_key(key: &str) -> String {
    if key.is_empty() {
        return "<empty>".to_string();
    }
    if key.len() <= 8 {
        return "***".to_string();
    }
    format!("{}...{}", &key[..4], &key[key.len() - 4..])
}

async fn sync_env_keys(pool: &PgPool) -> Result<(), sqlx::Error> {
    let Ok(raw) = std::env::var("QUAZAR_API_KEYS") else {
        return Ok(());
    };

    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = entry.split(':').map(str::trim).collect();
        if parts.len() != 3 {
            tracing::warn!(entry = %entry, "skipped API key: invalid format");
            continue;
        }
        let (key, role_str, citizen_name) = (parts[0], parts[1], parts[2]);
        let Some(role) = Role::from_str(role_str) else {
            tracing::warn!(entry = %entry, role = %role_str, "skipped API key: unknown role");
            continue;
        };
        KeyStore::upsert_key(pool, key, role, citizen_name).await?;
        tracing::info!(key = %mask_key(key), citizen = %citizen_name, role = %role_str, "synced API key from env");
    }
    Ok(())
}

async fn sync_test_keys(pool: &PgPool) -> Result<(), sqlx::Error> {
    let enabled = std::env::var("QUAZAR_INIT_TEST_KEYS")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if !enabled {
        return Ok(());
    }

    let test_keys = [
        ("test_citizen_key_2026", Role::Citizen, "test_citizen"),
        ("buyer_key_2026", Role::Citizen, "buyer_citizen"),
        ("seller_key_2026", Role::Citizen, "seller_citizen"),
    ];

    for (key, role, name) in &test_keys {
        KeyStore::upsert_key(pool, key, role.clone(), name).await?;
    }
    tracing::info!(count = test_keys.len(), "test API keys synced");
    Ok(())
}

pub const MASTER_NAME: &str = "successful";

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub citizen_name: String,
    pub role: Role,
    pub is_master: bool,
    pub is_node: bool,
}

impl AuthContext {
    pub fn master() -> Self {
        Self {
            citizen_name: MASTER_NAME.to_string(),
            role: Role::Aiya,
            is_master: true,
            is_node: false,
        }
    }

    pub fn node() -> Self {
        Self {
            citizen_name: "__node__".to_string(),
            role: Role::Guardian,
            is_master: false,
            is_node: true,
        }
    }

    pub fn from_api_key(citizen_name: String, role: Role) -> Self {
        Self {
            citizen_name,
            role,
            is_master: false,
            is_node: false,
        }
    }

    pub fn can_manage_peers(&self) -> bool {
        !self.is_node && (self.is_master || matches!(self.role, Role::Aiya | Role::Guardian))
    }

    pub fn can_add_balance(&self) -> bool {
        !self.is_node && (self.is_master || self.role == Role::Aiya)
    }

    pub fn can_assign_elevated_role(&self) -> bool {
        !self.is_node && (self.is_master || matches!(self.role, Role::Aiya | Role::Guardian))
    }

    pub fn can_manage_citizens(&self) -> bool {
        !self.is_node && (self.is_master || matches!(self.role, Role::Aiya | Role::Guardian))
    }

    pub fn can_manage_api_keys(&self) -> bool {
        !self.is_node && (self.is_master || self.role == Role::Aiya)
    }

    pub async fn authorize_citizen_ref(&self, db: &PgPool, citizen_ref: &str) -> bool {
        if self.is_node {
            return false;
        }
        if self.is_master {
            return true;
        }
        if self.citizen_name == citizen_ref {
            return true;
        }
        let db_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM citizens WHERE name = $1",
        )
        .bind(&self.citizen_name)
        .fetch_optional(db)
        .await
        .unwrap_or(None);
        db_id.as_deref() == Some(citizen_ref)
    }

    pub async fn resolve_account_id(&self, db: &PgPool) -> String {
        if self.is_node {
            return self.citizen_name.clone();
        }
        sqlx::query_scalar("SELECT id FROM citizens WHERE name = $1")
            .bind(&self.citizen_name)
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| self.citizen_name.clone())
    }
}
