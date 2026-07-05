use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    pub event_id: String,
    pub timestamp: i64,
    pub event_type: String,
    pub title: String,
    pub description: String,
    pub initiator: String,
    pub data: serde_json::Value,
    pub previous_hash: String,
    pub signatures: Vec<String>,
    pub hash: Option<String>,
    pub public: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    pub block_number: u64,
    pub timestamp: i64,
    pub events: Vec<Event>,
    pub previous_hash: String,
    pub block_hash: String,
    pub events_count: usize,
}

/// POST /event body: flat Event JSON, optional `{ "event": ... }`, legacy `key` in body is ignored.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum AddEventRequest {
    Wrapped {
        /// Legacy clients — ignored; use Authorization header.
        #[serde(default, rename = "key")]
        _legacy_key: Option<String>,
        event: Event,
    },
    Flat {
        /// Legacy clients — ignored; use Authorization header.
        #[serde(default, rename = "key")]
        _legacy_key: Option<String>,
        #[serde(flatten)]
        event: Event,
    },
}

impl AddEventRequest {
    pub fn into_event(self) -> Event {
        match self {
            AddEventRequest::Wrapped { event, .. } => event,
            AddEventRequest::Flat { event, .. } => event,
        }
    }
}
