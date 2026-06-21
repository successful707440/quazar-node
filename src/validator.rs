use crate::types::QuazarEventType;
use serde_json::Value;
use rusqlite::Connection;

#[derive(Debug)]
pub enum ValidationError {
    InvalidEventType,
    MissingRequiredField(String),
    InvalidCitizenName(String),
    DuplicateCitizenId,
    DuplicateCitizenName,
    InvalidPublicKey,
    DataValidationFailed(String),
}

pub struct EventValidator;

impl EventValidator {
    pub fn validate_citizen_name(name: &str, existing_names: &[String]) -> Result<(), ValidationError> {
        if !name.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(ValidationError::InvalidCitizenName(
                "Name must contain only Latin letters".to_string()
            ));
        }
        if name.is_empty() {
            return Err(ValidationError::InvalidCitizenName(
                "Name cannot be empty".to_string()
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
                let name = data.get("citizen_name")
                    .and_then(|v| v.as_str())
                    .ok_or(ValidationError::MissingRequiredField("citizen_name".to_string()))?;
                Self::validate_citizen_name(name, citizens)?;
                let _birth_place = data.get("birth_place")
                    .ok_or(ValidationError::MissingRequiredField("birth_place".to_string()))?;
                let _public_key = data.get("public_key")
                    .ok_or(ValidationError::MissingRequiredField("public_key".to_string()))?;
                Ok(())
            },
            QuazarEventType::PassportIssued => {
                let _citizen_id = data.get("citizen_id")
                    .ok_or(ValidationError::MissingRequiredField("citizen_id".to_string()))?;
                let _expires_at = data.get("expires_at")
                    .ok_or(ValidationError::MissingRequiredField("expires_at".to_string()))?;
                Ok(())
            },
            QuazarEventType::LawAdded => {
                let _law_id = data.get("law_id")
                    .ok_or(ValidationError::MissingRequiredField("law_id".to_string()))?;
                let _title = data.get("title")
                    .ok_or(ValidationError::MissingRequiredField("title".to_string()))?;
                let _content = data.get("content")
                    .ok_or(ValidationError::MissingRequiredField("content".to_string()))?;
                Ok(())
            },
            QuazarEventType::AiyaElected => {
                let _new_aiya = data.get("new_aiya")
                    .ok_or(ValidationError::MissingRequiredField("new_aiya".to_string()))?;
                let _votes = data.get("votes")
                    .ok_or(ValidationError::MissingRequiredField("votes".to_string()))?;
                Ok(())
            },
            _ => Ok(()),
        }
    }
    
    pub fn get_citizens(db: &Connection) -> Result<Vec<String>, rusqlite::Error> {
        let mut stmt = db.prepare(
            "SELECT json_extract(data, '$.citizen_name') as name 
             FROM events 
             WHERE event_type = 'CitizenAdded'
             ORDER BY timestamp DESC"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;
        let mut names = Vec::new();
        for row in rows {
            names.push(row?);
        }
        Ok(names)
    }
}
