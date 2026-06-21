use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use reqwest::Client;
use crate::nodes::NodeRegistry;
use rusqlite::Connection;

// Импортируем структуры из main.rs через crate
use crate::Event;
use crate::Block;

pub struct SyncManager {
    client: Client,
    node_registry: Arc<Mutex<NodeRegistry>>,
    my_url: String,
    my_id: String,
}

impl SyncManager {
    pub fn new(node_registry: Arc<Mutex<NodeRegistry>>, my_url: String, my_id: String) -> Self {
        Self {
            client: Client::new(),
            node_registry,
            my_url,
            my_id,
        }
    }
    
    pub async fn sync_with_peer(&self, peer_url: &str, db: Arc<Mutex<Connection>>) -> Result<(), String> {
        let blocks_url = format!("{}/blocks", peer_url);
        match self.client.get(&blocks_url).timeout(std::time::Duration::from_secs(5)).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let peer_blocks: Vec<Block> = response.json().await.map_err(|e| e.to_string())?;
                    println!("📦 Received {} blocks from peer: {}", peer_blocks.len(), peer_url);
                    
                    let db = db.lock().await;
                    for block in peer_blocks {
                        let exists: bool = db.query_row(
                            "SELECT COUNT(*) FROM blocks WHERE block_number = ?1",
                            [&block.block_number.to_string()],
                            |row| row.get(0)
                        ).unwrap_or(0) > 0;
                        
                        if !exists {
                            let block_json = serde_json::to_string(&block).unwrap();
                            db.execute(
                                "INSERT INTO blocks (block_number, block_hash, previous_hash, timestamp, block_data, events_count, created_at)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                [
                                    &block.block_number.to_string(),
                                    &block.block_hash,
                                    &block.previous_hash,
                                    &block.timestamp.to_string(),
                                    &block_json,
                                    &block.events_count.to_string(),
                                    &chrono::Utc::now().to_rfc3339(),
                                ],
                            ).map_err(|e| e.to_string())?;
                            println!("✅ Synced block #{} from peer", block.block_number);
                            
                            for event in block.events {
                                let data_json = serde_json::to_string(&event.data).unwrap();
                                let sig_json = serde_json::to_string(&event.signatures).unwrap();
                                let _ = db.execute(
                                    "INSERT OR IGNORE INTO events (event_id, timestamp, event_type, title, description, initiator, data, previous_hash, signatures, hash, created_at, public)
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                                    [
                                        &event.event_id, &event.timestamp.to_string(), &event.event_type,
                                        &event.title, &event.description, &event.initiator, &data_json,
                                        &event.previous_hash, &sig_json, &event.hash.as_ref().unwrap_or(&"".to_string()),
                                        &chrono::Utc::now().to_rfc3339(), &event.public.to_string(),
                                    ],
                                );
                            }
                        }
                    }
                    println!("✅ Sync completed with peer: {}", peer_url);
                } else {
                    println!("❌ Failed to sync with peer: {} - status: {}", peer_url, response.status());
                }
            },
            Err(e) => {
                println!("❌ Failed to connect to peer: {} - error: {}", peer_url, e);
            }
        }
        Ok(())
    }
    
    pub async fn sync_with_all_peers(&self, db: Arc<Mutex<Connection>>) {
        let registry = self.node_registry.lock().await;
        let peers = registry.get_all_nodes().unwrap_or_default();
        
        for peer in peers {
            if peer.id != self.my_id {
                println!("🔄 Syncing with peer: {} ({})", peer.id, peer.url);
                let _ = self.sync_with_peer(&peer.url, db.clone()).await;
            }
        }
    }
}
