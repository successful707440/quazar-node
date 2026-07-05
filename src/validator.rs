use crate::types::QuazarEventType;
use crate::models::Event;
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug)]
pub enum ValidationError {
    InvalidEventType(String),
    MissingRequiredField(String),
    InvalidCitizenName(String),
    DuplicateCitizenName,
    DuplicateEventId,
    InvalidPublicKey,
    DataValidationFailed(String),
    HashMismatch,
}

impl ValidationError {
    pub fn message(&self) -> String {
        match self {
            ValidationError::InvalidEventType(t) => format!("Unknown event_type: {}", t),
            ValidationError::MissingRequiredField(f) => format!("Missing required field: {}", f),
            ValidationError::InvalidCitizenName(msg) => msg.clone(),
            ValidationError::DuplicateCitizenName => "Citizen name already exists".to_string(),
            ValidationError::DuplicateEventId => format!("event_id already exists"),
            ValidationError::InvalidPublicKey => "Invalid public_key".to_string(),
            ValidationError::DataValidationFailed(msg) => msg.clone(),
            ValidationError::HashMismatch => "Event hash does not match content".to_string(),
        }
    }
}

pub struct EventValidator;

impl EventValidator {
    fn require_str<'a>(data: &'a Value, field: &str) -> Result<&'a str, ValidationError> {
        data.get(field)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ValidationError::MissingRequiredField(field.to_string()))
    }

    pub fn validate_citizen_name(name: &str, existing_names: &[String]) -> Result<(), ValidationError> {
        if name.is_empty() {
            return Err(ValidationError::InvalidCitizenName(
                "Name cannot be empty".to_string(),
            ));
        }
        if !name.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(ValidationError::InvalidCitizenName(
                "Name must contain only Latin letters".to_string(),
            ));
        }
        if existing_names.contains(&name.to_string()) {
            return Err(ValidationError::DuplicateCitizenName);
        }
        Ok(())
    }

    pub fn validate_event_data(
        event_type: &QuazarEventType,
        data: &Value,
        citizens: &[String],
    ) -> Result<(), ValidationError> {
        match event_type {
            QuazarEventType::CitizenAdded => {
                let name = data
                    .get("citizen_name")
                    .and_then(|v| v.as_str())
                    .ok_or(ValidationError::MissingRequiredField("citizen_name".to_string()))?;
                Self::validate_citizen_name(name, citizens)?;
                data.get("birth_place")
                    .ok_or(ValidationError::MissingRequiredField("birth_place".to_string()))?;
                let public_key = data
                    .get("public_key")
                    .and_then(|v| v.as_str())
                    .ok_or(ValidationError::MissingRequiredField("public_key".to_string()))?;
                crate::crypto::validate_public_key_hex(public_key)?;
                Ok(())
            }
            QuazarEventType::PassportIssued => {
                Self::require_str(data, "citizen_id")?;
                Self::require_str(data, "passport_id")?;
                if data.get("expires_at").is_none() {
                    return Err(ValidationError::MissingRequiredField("expires_at".to_string()));
                }
                Ok(())
            }
            QuazarEventType::LawAdded => {
                data.get("law_id")
                    .ok_or(ValidationError::MissingRequiredField("law_id".to_string()))?;
                data.get("title")
                    .ok_or(ValidationError::MissingRequiredField("title".to_string()))?;
                data.get("content")
                    .ok_or(ValidationError::MissingRequiredField("content".to_string()))?;
                Ok(())
            }
            QuazarEventType::AiyaElected => {
                data.get("new_aiya")
                    .ok_or(ValidationError::MissingRequiredField("new_aiya".to_string()))?;
                data.get("votes")
                    .ok_or(ValidationError::MissingRequiredField("votes".to_string()))?;
                Ok(())
            }
            QuazarEventType::PeerListUpdate => {
                let peers = data.get("peers").and_then(|v| v.as_array()).ok_or(
                    ValidationError::MissingRequiredField("peers".to_string()),
                )?;
                if peers.is_empty() {
                    return Err(ValidationError::DataValidationFailed(
                        "peers array must not be empty".to_string(),
                    ));
                }
                for peer in peers {
                    peer.get("id")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .ok_or(ValidationError::MissingRequiredField(
                            "peers[].id".to_string(),
                        ))?;
                    peer.get("url")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .ok_or(ValidationError::MissingRequiredField(
                            "peers[].url".to_string(),
                        ))?;
                }
                Ok(())
            }
            QuazarEventType::VoteCast => {
                data.get("vote_id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(ValidationError::MissingRequiredField("vote_id".to_string()))?;
                data.get("citizen_id")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(ValidationError::MissingRequiredField("citizen_id".to_string()))?;
                data.get("choice")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(ValidationError::MissingRequiredField("choice".to_string()))?;
                Ok(())
            }
            QuazarEventType::CitizenRemoved | QuazarEventType::CitizenUpdated => {
                Self::require_str(data, "citizen_id")?;
                if matches!(event_type, QuazarEventType::CitizenUpdated) {
                    Self::require_str(data, "status")?;
                }
                Ok(())
            }
            QuazarEventType::NodeAdded => {
                data.get("node_id")
                    .or_else(|| data.get("id"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(ValidationError::MissingRequiredField("node_id".to_string()))?;
                data.get("url")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or(ValidationError::MissingRequiredField("url".to_string()))?;
                Ok(())
            }
            QuazarEventType::SystemInit | QuazarEventType::SystemUpgrade => {
                Self::require_str(data, "version")?;
                Ok(())
            }
            QuazarEventType::SystemConfig => {
                let has_config_key = data
                    .get("config_key")
                    .or_else(|| data.get("key"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .is_some();
                if !has_config_key {
                    return Err(ValidationError::MissingRequiredField(
                        "config_key (setting name in event data, not the API Authorization key)"
                            .to_string(),
                    ));
                }
                Self::require_str(data, "value")?;
                Ok(())
            }
            QuazarEventType::CitizenRequested => {
                let name = Self::require_str(data, "citizen_name")?;
                Self::validate_citizen_name(name, citizens)?;
                Self::require_str(data, "public_key")?;
                Ok(())
            }
            QuazarEventType::CitizenSuspended | QuazarEventType::CitizenRestored => {
                Self::require_str(data, "citizen_id")?;
                Ok(())
            }
            QuazarEventType::PassportSuspended | QuazarEventType::PassportRevoked => {
                Self::require_str(data, "citizen_id")?;
                Self::require_str(data, "passport_id")?;
                Ok(())
            }
            QuazarEventType::LawProposed | QuazarEventType::LawAmended | QuazarEventType::LawRepealed => {
                Self::require_str(data, "law_id")?;
                Ok(())
            }
            QuazarEventType::LawVoteStarted | QuazarEventType::LawVoteResult => {
                Self::require_str(data, "law_id")?;
                Self::require_str(data, "vote_id")?;
                Ok(())
            }
            QuazarEventType::ElectionAnnounced => {
                Self::require_str(data, "election_id")?;
                Ok(())
            }
            QuazarEventType::ElectionCandidate => {
                Self::require_str(data, "election_id")?;
                Self::require_str(data, "candidate_id")?;
                Ok(())
            }
            QuazarEventType::ElectionVoteStarted | QuazarEventType::ElectionVoteResult => {
                Self::require_str(data, "election_id")?;
                Self::require_str(data, "vote_id")?;
                Ok(())
            }
            QuazarEventType::AppointmentGuardian | QuazarEventType::AppointmentJudge => {
                Self::require_str(data, "citizen_id")?;
                Self::require_str(data, "appointed_by")?;
                Ok(())
            }
            QuazarEventType::AppointmentRevoked => {
                Self::require_str(data, "citizen_id")?;
                Self::require_str(data, "role")?;
                Ok(())
            }
            QuazarEventType::CourtCaseOpened | QuazarEventType::CourtRuling
            | QuazarEventType::CourtAppeal | QuazarEventType::CourtAppealRuling => {
                Self::require_str(data, "case_id")?;
                Ok(())
            }
            QuazarEventType::DomainRegistered | QuazarEventType::DomainTransferred
            | QuazarEventType::DomainExpired => {
                Self::require_str(data, "domain")?;
                Ok(())
            }
            QuazarEventType::NodeRemoved => {
                Self::require_str(data, "node_id")?;
                Ok(())
            }
            QuazarEventType::InfraMigration => {
                Self::require_str(data, "target")?;
                Ok(())
            }
            QuazarEventType::ConstitutionFullText => {
                Self::require_str(data, "text")?;
                Ok(())
            }
            QuazarEventType::VoteStarted => {
                Self::require_str(data, "vote_id")?;
                Self::require_str(data, "title")?;
                Ok(())
            }
            QuazarEventType::VoteFinalized => {
                Self::require_str(data, "vote_id")?;
                Ok(())
            }
        }
    }

    pub async fn validate_event(event: &Event, db: &PgPool) -> Result<(), ValidationError> {
        if event.event_id.trim().is_empty() {
            return Err(ValidationError::MissingRequiredField("event_id".to_string()));
        }
        if event.title.trim().is_empty() {
            return Err(ValidationError::MissingRequiredField("title".to_string()));
        }
        if event.description.trim().is_empty() {
            return Err(ValidationError::MissingRequiredField("description".to_string()));
        }
        if event.initiator.trim().is_empty() {
            return Err(ValidationError::MissingRequiredField("initiator".to_string()));
        }
        if event.timestamp <= 0 {
            return Err(ValidationError::DataValidationFailed(
                "timestamp must be positive".to_string(),
            ));
        }

        if event_id_exists(db, &event.event_id).await {
            return Err(ValidationError::DuplicateEventId);
        }

        let event_type = QuazarEventType::from_str(&event.event_type)
            .map_err(ValidationError::InvalidEventType)?;

        let citizens = get_all_citizen_names(db).await.unwrap_or_default();
        Self::validate_event_data(&event_type, &event.data, &citizens)?;

        let event_hash = crate::blockchain::compute_event_hash(event);
        if let Some(provided) = event.hash.as_ref().filter(|h| !h.is_empty()) {
            if provided != &event_hash {
                return Err(ValidationError::HashMismatch);
            }
        }

        crate::crypto::verify_event_signatures(event, &event_hash, db).await?;

        Ok(())
    }
}

async fn event_id_exists(db: &PgPool, event_id: &str) -> bool {
    let in_pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pending_events WHERE event_id = $1)",
    )
    .bind(event_id)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    let in_events: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)",
    )
    .bind(event_id)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    in_pending || in_events
}

async fn get_all_citizen_names(db: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT name FROM citizens
        UNION
        SELECT data::jsonb->>'citizen_name' FROM events
            WHERE event_type = 'CitizenAdded'
              AND data::jsonb->>'citizen_name' IS NOT NULL
        UNION
        SELECT event_data::jsonb->'data'->>'citizen_name' FROM pending_events
            WHERE event_data::jsonb->>'event_type' = 'CitizenAdded'
              AND event_data::jsonb->'data'->>'citizen_name' IS NOT NULL
        "#,
    )
    .fetch_all(db)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_citizen_name_rejects_empty() {
        let err = EventValidator::validate_citizen_name("", &[]).unwrap_err();
        assert!(matches!(err, ValidationError::InvalidCitizenName(_)));
    }

    #[test]
    fn validate_citizen_name_rejects_non_latin() {
        let err = EventValidator::validate_citizen_name("alice123", &[]).unwrap_err();
        assert!(matches!(err, ValidationError::InvalidCitizenName(_)));
    }

    #[test]
    fn validate_citizen_name_rejects_duplicate() {
        let existing = vec!["alice".to_string()];
        let err = EventValidator::validate_citizen_name("alice", &existing).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateCitizenName));
    }

    #[test]
    fn validate_citizen_name_accepts_unique_latin() {
        assert!(EventValidator::validate_citizen_name("bob", &["alice".to_string()]).is_ok());
    }
}
