use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::auth::{internal_node_signature, registration_signature};
use crate::models::Event;
use crate::validator::ValidationError;

const ED25519_SIG_PREFIX: &str = "ed25519_";
const PUBLIC_KEY_HEX_LEN: usize = 64;

/// Normalize hex key: trim, strip optional `0x`/`0X`, lowercase.
pub fn normalize_public_key_hex(key: &str) -> Result<String, ValidationError> {
    let mut s = key.trim().to_string();
    if s.starts_with("0x") || s.starts_with("0X") {
        s = s[2..].to_string();
    }
    if s.len() != PUBLIC_KEY_HEX_LEN {
        return Err(ValidationError::InvalidPublicKey);
    }
    if !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ValidationError::InvalidPublicKey);
    }
    Ok(s.to_ascii_lowercase())
}

/// Format check for registration: 64 hex chars (optional 0x prefix), decodes to 32 bytes.
pub fn validate_public_key_hex(key: &str) -> Result<String, ValidationError> {
    let normalized = normalize_public_key_hex(key)?;
    let bytes = hex::decode(&normalized).map_err(|_| ValidationError::InvalidPublicKey)?;
    if bytes.len() != 32 {
        return Err(ValidationError::InvalidPublicKey);
    }
    Ok(normalized)
}

/// Strict Ed25519 point validation (for signature verification).
pub fn validate_ed25519_public_key_hex(key: &str) -> Result<VerifyingKey, ValidationError> {
    let normalized = validate_public_key_hex(key)?;
    let bytes = hex::decode(&normalized).map_err(|_| ValidationError::InvalidPublicKey)?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ValidationError::InvalidPublicKey)?;
    VerifyingKey::from_bytes(&arr).map_err(|_| ValidationError::InvalidPublicKey)
}

pub fn sign_event_hash_ed25519(
    private_key_seed_hex: &str,
    event_hash: &str,
) -> Result<String, ValidationError> {
    let seed_bytes = hex::decode(private_key_seed_hex.trim())
        .map_err(|_| ValidationError::DataValidationFailed("Invalid private key hex".into()))?;
    if seed_bytes.len() != 32 {
        return Err(ValidationError::DataValidationFailed(
            "Private key must be 32 bytes hex".into(),
        ));
    }
    let seed: [u8; 32] = seed_bytes
        .try_into()
        .map_err(|_| ValidationError::DataValidationFailed("Invalid private key".into()))?;
    let signing_key = SigningKey::from_bytes(&seed);
    let signature = signing_key.sign(event_hash.as_bytes());
    Ok(format!(
        "{}{}",
        ED25519_SIG_PREFIX,
        hex::encode(signature.to_bytes())
    ))
}

fn verify_ed25519(public_key_hex: &str, event_hash: &str, signature: &str) -> Result<(), ValidationError> {
    let Some(sig_hex) = signature.strip_prefix(ED25519_SIG_PREFIX) else {
        return Err(ValidationError::DataValidationFailed(
            "Expected ed25519_ signature prefix".into(),
        ));
    };
    let verifying_key = validate_ed25519_public_key_hex(public_key_hex)?;
    let sig_bytes = hex::decode(sig_hex).map_err(|_| {
        ValidationError::DataValidationFailed("Invalid ed25519 signature hex".into())
    })?;
    if sig_bytes.len() != 64 {
        return Err(ValidationError::DataValidationFailed(
            "Ed25519 signature must be 64 bytes".into(),
        ));
    }
    let sig_arr: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| ValidationError::DataValidationFailed("Invalid signature length".into()))?;
    let sig = Signature::from_bytes(&sig_arr);
    verifying_key
        .verify(event_hash.as_bytes(), &sig)
        .map_err(|_| {
            ValidationError::DataValidationFailed("Ed25519 signature verification failed".into())
        })
}

fn verify_registration_sig(
    citizen_id: &str,
    public_key: &str,
    event_hash: &str,
    signature: &str,
) -> bool {
    signature == registration_signature(citizen_id, public_key, event_hash)
}

fn verify_node_sig(event_id: &str, event_hash: &str, signature: &str) -> bool {
    signature == internal_node_signature(event_id, event_hash)
}

pub async fn verify_event_signatures(
    event: &Event,
    event_hash: &str,
    db: &sqlx::PgPool,
) -> Result<(), ValidationError> {
    let signatures: Vec<&String> = event
        .signatures
        .iter()
        .filter(|s| !s.trim().is_empty())
        .collect();

    if signatures.is_empty() {
        return Err(ValidationError::MissingRequiredField("signatures".into()));
    }

    for sig in &signatures {
        if *sig == "admin_sig" || sig.len() < 16 {
            return Err(ValidationError::DataValidationFailed(
                "Invalid or placeholder signature".into(),
            ));
        }
    }

    for sig in signatures {
        if sig.starts_with("node_sig_") {
            if verify_node_sig(&event.event_id, event_hash, sig) {
                return Ok(());
            }
            continue;
        }
        if sig.starts_with("reg_sig_") {
            if event.event_type == "CitizenAdded" {
                let citizen_id = event
                    .data
                    .get("citizen_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let public_key = event
                    .data
                    .get("public_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !citizen_id.is_empty()
                    && !public_key.is_empty()
                    && verify_registration_sig(citizen_id, public_key, event_hash, sig)
                {
                    return Ok(());
                }
            }
            continue;
        }
        if sig.starts_with(ED25519_SIG_PREFIX) {
            if let Some(public_key) = resolve_signer_public_key(event, db).await {
                if verify_ed25519(&public_key, event_hash, sig).is_ok() {
                    return Ok(());
                }
            }
        }
    }

    Err(ValidationError::DataValidationFailed(
        "No valid cryptographic signature found for event".into(),
    ))
}

async fn resolve_signer_public_key(event: &Event, db: &sqlx::PgPool) -> Option<String> {
    if event.event_type == "CitizenAdded" {
        if let Some(pk) = event.data.get("public_key").and_then(|v| v.as_str()) {
            return Some(pk.to_string());
        }
    }

    let by_name: Option<String> = sqlx::query_scalar(
        "SELECT public_key FROM citizens WHERE name = $1",
    )
    .bind(&event.initiator)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    if by_name.is_some() {
        return by_name;
    }

    sqlx::query_scalar("SELECT public_key FROM citizens WHERE id = $1")
        .bind(&event.initiator)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
}

pub fn is_weak_secret(value: &str) -> bool {
    matches!(
        value,
        "QUAZAR_MASTER_KEY_2026"
            | "QUAZAR_NODE_SECRET_2026"
            | "QUAZAR_REG_SECRET_2026"
            | "QUAZAR_MASTER_KEY_CI"
            | "QUAZAR_NODE_SECRET_CI"
            | "QUAZAR_REG_SECRET_CI"
    )
}

pub fn assert_production_secrets(master: &str, node_secret: &str, reg_secret: &str) {
    let strict = std::env::var("QUAZAR_STRICT_SECRETS")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if is_weak_secret(master) {
        if strict {
            panic!("QUAZAR_STRICT_SECRETS: QUAZAR_MASTER_KEY uses a known default value");
        }
        tracing::warn!("QUAZAR_MASTER_KEY matches a known default — unsafe for production");
    }
    if is_weak_secret(node_secret) {
        if strict {
            panic!("QUAZAR_STRICT_SECRETS: QUAZAR_NODE_SECRET uses a known default value");
        }
        tracing::warn!("QUAZAR_NODE_SECRET matches a known default — unsafe for production");
    }
    if is_weak_secret(reg_secret) {
        if strict {
            panic!("QUAZAR_STRICT_SECRETS: QUAZAR_REG_SECRET uses a known default value");
        }
        tracing::warn!("QUAZAR_REG_SECRET matches a known default — unsafe for production");
    }
    if reg_secret == node_secret {
        if strict {
            panic!("QUAZAR_STRICT_SECRETS: QUAZAR_REG_SECRET must differ from QUAZAR_NODE_SECRET");
        }
        tracing::warn!("QUAZAR_REG_SECRET equals QUAZAR_NODE_SECRET — reg_sig can be forged with P2P secret");
    }
    if strict {
        let master_disabled = std::env::var("QUAZAR_DISABLE_MASTER_KEY")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        if !master_disabled {
            panic!("QUAZAR_STRICT_SECRETS: set QUAZAR_DISABLE_MASTER_KEY=true in production");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &str = "9d61b19deffd5a60ba844af492ec2cc4c419848d1e3bef484bfe8bf377712000";
    #[test]
    fn ed25519_sign_and_verify() {
        let seed = hex::decode(TEST_SECRET).unwrap();
        let seed: [u8; 32] = seed.try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&seed);
        let public = hex::encode(signing_key.verifying_key().to_bytes());
        let hash = "abc123";
        let sig = sign_event_hash_ed25519(TEST_SECRET, hash).unwrap();
        verify_ed25519(&public, hash, &sig).unwrap();
    }

    #[test]
    fn validate_public_key_hex_accepts_registration_keys() {
        for key in [
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986",
            "70f2ce7adba0baa9960b660f45a6b00fcdb5fe8353f4cdd4aa3a0cbb765945d5",
        ] {
            assert!(validate_public_key_hex(key).is_ok(), "key: {}", key);
        }
    }

    #[test]
    fn validate_public_key_hex_accepts_0x_prefix_and_uppercase() {
        let key = validate_public_key_hex(
            "0xD75A980182B10AB7D54BFED3C964073A0EE172F3DAA62325AF021A68F8077986",
        )
        .unwrap();
        assert_eq!(
            key,
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986"
        );
    }

    #[test]
    fn validate_public_key_hex_rejects_invalid_format() {
        assert!(validate_public_key_hex("not-a-key").is_err());
        assert!(validate_public_key_hex("abcd").is_err());
        assert!(validate_public_key_hex(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        )
        .is_err());
    }

    #[test]
    fn validate_ed25519_rejects_non_curve_point_hex() {
        assert!(validate_ed25519_public_key_hex(
            "70f2ce7adba0baa9960b660f45a6b00fcdb5fe8353f4cdd4aa3a0cbb765945d5"
        )
        .is_err());
        assert!(validate_ed25519_public_key_hex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986"
        )
        .is_ok());
    }

    #[test]
    fn rejects_invalid_public_key() {
        assert!(validate_ed25519_public_key_hex("not-a-key").is_err());
    }

    #[test]
    fn reg_sig_differs_from_node_sig_for_same_payload() {
        use crate::auth::{compute_node_signature, compute_registration_signature};
        let reg = compute_registration_signature("cid", "pk", "hash", "reg-secret");
        let node = compute_node_signature("cid", "hash", "node-secret");
        assert!(reg.starts_with("reg_sig_"));
        assert!(node.starts_with("node_sig_"));
        assert_ne!(reg, node);
        assert_ne!(
            compute_registration_signature("cid", "pk", "hash", "secret-a"),
            compute_registration_signature("cid", "pk", "hash", "secret-b")
        );
    }
}
