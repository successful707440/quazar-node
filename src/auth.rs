use std::collections::HashMap;
use sha2::{Sha256, Digest};

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Aiya,
    Guardian,
    Judge,
    Citizen,
}

pub struct ApiKey {
    pub key: String,
    pub role: Role,
    pub citizen_name: String,
}

pub struct KeyStore {
    keys: HashMap<String, ApiKey>,
}

impl KeyStore {
    pub fn new() -> Self {
        let mut keys = HashMap::new();
        keys.insert(
            "aiya_master_key_2024".to_string(),
            ApiKey {
                key: "aiya_master_key_2024".to_string(),
                role: Role::Aiya,
                citizen_name: "Successful".to_string(),
            },
        );
        Self { keys }
    }

    pub fn validate_key(&self, key: &str) -> Option<&ApiKey> {
        self.keys.get(key)
    }

    pub fn add_key(&mut self, key: String, role: Role, citizen_name: String) {
        let key_clone = key.clone();
        self.keys.insert(key, ApiKey { key: key_clone, role, citizen_name });
    }

    pub fn remove_key(&mut self, key: &str) {
        self.keys.remove(key);
    }
}

pub fn check_access(role: &Role, required_role: &Role) -> bool {
    match required_role {
        Role::Aiya => matches!(role, Role::Aiya),
        Role::Guardian => matches!(role, Role::Aiya | Role::Guardian),
        Role::Judge => matches!(role, Role::Aiya | Role::Guardian | Role::Judge),
        Role::Citizen => true,
    }
}

pub fn generate_api_key(citizen_name: &str, seed: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{}{}{}", citizen_name, seed, chrono::Utc::now().timestamp()).as_bytes());
    format!("{:x}", hasher.finalize())
}
