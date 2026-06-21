use axum::{
    Router, 
    routing::{get, post},
    response::Json,
    extract::State,
};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::net::SocketAddr;
use rusqlite::Connection;
use chrono::Utc;
use tokio::sync::Mutex;
use sha2::{Sha256, Digest};
use std::time::Duration;
use tokio::time;
use std::collections::VecDeque;

mod types;
mod auth;
mod validator;
mod nodes;
mod sync;

use types::QuazarEventType;
use auth::{KeyStore, Role, check_access};
use validator::EventValidator;
use nodes::{NodeRegistry, Node, NodeStatus};
use sync::SyncManager;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Event {
    event_id: String,
    timestamp: i64,
    event_type: String,
    title: String,
    description: String,
    initiator: String,
    data: serde_json::Value,
    previous_hash: String,
    signatures: Vec<String>,
    hash: Option<String>,
    public: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Block {
    block_number: u64,
    timestamp: i64,
    events: Vec<Event>,
    previous_hash: String,
    block_hash: String,
    events_count: usize,
}

#[derive(Serialize)]
struct AddEventResponse {
    status: String,
    event_id: String,
    message: String,
}

struct AppState {
    db: Arc<Mutex<Connection>>,
    keystore: Arc<Mutex<KeyStore>>,
    node_registry: Arc<Mutex<NodeRegistry>>,
    sync_manager: Arc<SyncManager>,
    node_id: String,
    node_url: String,
    pending_events: Arc<Mutex<VecDeque<Event>>>,
}

fn compute_hash(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn compute_event_hash(event: &Event) -> String {
    let content = format!(
        "{}{}{}{}{}{}{}{}{}",
        event.event_id, event.timestamp, event.event_type, event.title,
        event.description, event.initiator, event.previous_hash,
        serde_json::to_string(&event.data).unwrap_or_default(), event.public
    );
    compute_hash(&content)
}

async fn add_event(
    State(state): State<Arc<AppState>>,
    Json(mut event): Json<Event>,
) -> Json<AddEventResponse> {
    let hash = compute_event_hash(&event);
    event.hash = Some(hash);
    
    let mut pending = state.pending_events.lock().await;
    pending.push_back(event.clone());
    let count = pending.len();
    
    println!("📝 Event added: {} (pending: {})", event.event_id, count);
    
    Json(AddEventResponse {
        status: "pending".to_string(),
        event_id: event.event_id,
        message: format!("Event added ({} events waiting)", count),
    })
}

async fn create_block(state: Arc<AppState>) -> Result<Block, String> {
    let mut pending = state.pending_events.lock().await;
    if pending.is_empty() {
        return Err("No pending events".to_string());
    }
    
    let events: Vec<Event> = pending.drain(..).collect();
    let events_count = events.len();
    
    println!("📦 Creating block with {} events...", events_count);
    
    let db = state.db.lock().await;
    
    for event in &events {
        let data_json = serde_json::to_string(&event.data).unwrap();
        let sig_json = serde_json::to_string(&event.signatures).unwrap();
        let _ = db.execute(
            "INSERT INTO events (event_id, timestamp, event_type, title, description, initiator, data, previous_hash, signatures, hash, created_at, public)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            [
                &event.event_id, &event.timestamp.to_string(), &event.event_type,
                &event.title, &event.description, &event.initiator, &data_json,
                &event.previous_hash, &sig_json, &event.hash.as_ref().unwrap_or(&"".to_string()),
                &Utc::now().to_rfc3339(), &event.public.to_string(),
            ],
        );
    }
    
    let last_hash: String = db.query_row(
        "SELECT block_hash FROM blocks ORDER BY block_number DESC LIMIT 1",
        [],
        |row| row.get(0)
    ).unwrap_or_else(|_| "0".to_string());
    
    let block_number = db.query_row(
        "SELECT COUNT(*) FROM blocks",
        [],
        |row| row.get::<_, u64>(0)
    ).unwrap_or(0) + 1;
    
    let block = Block {
        block_number,
        timestamp: Utc::now().timestamp(),
        events: events.clone(),
        previous_hash: last_hash,
        block_hash: "".to_string(),
        events_count,
    };
    
    let block_hash = compute_hash(&format!(
        "{}{}{}{}",
        block.block_number,
        block.timestamp,
        block.events_count,
        block.previous_hash
    ));
    
    let mut final_block = block;
    final_block.block_hash = block_hash.clone();
    
    let block_json = serde_json::to_string(&final_block).unwrap();
    let now = Utc::now().to_rfc3339();
    
    match db.execute(
        "INSERT INTO blocks (block_number, block_hash, previous_hash, timestamp, block_data, events_count, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        [
            &final_block.block_number.to_string(),
            &final_block.block_hash,
            &final_block.previous_hash,
            &final_block.timestamp.to_string(),
            &block_json,
            &final_block.events_count.to_string(),
            &now,
        ],
    ) {
        Ok(_) => println!("✅ Block #{} saved!", block_number),
        Err(e) => println!("❌ Failed to save block: {}", e),
    }

    // 🔥 АВТО-ДОБАВЛЕНИЕ ПИРОВ ИЗ БЛОКА
    for event in &events {
        if event.event_type == "PeerListUpdate" {
            println!("🔄 Авто-добавление пиров из события: {}", event.event_id);
            if let Some(peers) = event.data.get("peers").and_then(|v| v.as_array()) {
                let registry = state.node_registry.lock().await;
                for peer_data in peers {
                    if let (Some(id), Some(url), Some(status_str)) = (
                        peer_data.get("id").and_then(|v| v.as_str()),
                        peer_data.get("url").and_then(|v| v.as_str()),
                        peer_data.get("status").and_then(|v| v.as_str()),
                    ) {
                        let status = match status_str {
                            "alive" => NodeStatus::Alive,
                            "dead" => NodeStatus::Dead,
                            _ => NodeStatus::Alive,
                        };
                        let peer = Node {
                            id: id.to_string(),
                            url: url.to_string(),
                            public_key: None,
                            status,
                            last_seen: chrono::Utc::now().to_rfc3339(),
                            version: "0.7.0".to_string(),
                        };
                        let _ = registry.upsert_node(&peer);
                        println!("✅ Авто-добавлен пир из блока: {} ({})", id, url);
                    }
                }
                drop(registry);
            }
        }
    }

    Ok(final_block)
}

// ===== ЭНДПОИНТЫ =====

async fn get_events(State(state): State<Arc<AppState>>) -> Json<Vec<Event>> {
    let pending = state.pending_events.lock().await;
    let events: Vec<Event> = pending.iter().cloned().collect();
    println!("📱 Feed: {} pending events", events.len());
    Json(events)
}

async fn get_blocks(State(state): State<Arc<AppState>>) -> Json<Vec<Block>> {
    let db = state.db.lock().await;
    let mut stmt = db.prepare("SELECT block_data FROM blocks ORDER BY block_number ASC").unwrap();
    let rows = stmt.query_map([], |row| {
        let data: String = row.get(0)?;
        serde_json::from_str(&data).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))
    }).unwrap();
    let blocks: Vec<Block> = rows.filter_map(|r| r.ok()).collect();
    println!("📚 Returning {} blocks", blocks.len());
    Json(blocks)
}

async fn status() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok", "version": "0.7.0",
        "message": "Quazar Blockchain - feed shows pending events",
        "blockchain": true
    }))
}

async fn get_nodes(State(state): State<Arc<AppState>>) -> Json<Vec<Node>> {
    let registry = state.node_registry.lock().await;
    let nodes = registry.get_all_nodes().unwrap_or_default();
    Json(nodes)
}

async fn add_peer(
    State(state): State<Arc<AppState>>,
    Json(peer): Json<Node>,
) -> Json<serde_json::Value> {
    let registry = state.node_registry.lock().await;
    match registry.upsert_node(&peer) {
        Ok(_) => Json(serde_json::json!({"status": "ok", "message": "Peer added"})),
        Err(e) => Json(serde_json::json!({"status": "error", "message": format!("Failed: {}", e)})),
    }
}

async fn online_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let citizen_id = payload.get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    let db = state.db.lock().await;
    let now = Utc::now().timestamp();
    
    let _ = db.execute(
        "INSERT OR REPLACE INTO citizen_status (citizen_id, status, last_seen) VALUES (?1, 'online', ?2)",
        [&citizen_id, &now.to_string()],
    );
    
    Json(serde_json::json!({
        "status": "ok",
        "citizen_id": citizen_id,
        "message": "Citizen marked as online"
    }))
}

async fn offline_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let citizen_id = payload.get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    let db = state.db.lock().await;
    let _ = db.execute(
        "UPDATE citizen_status SET status = 'offline', last_seen = ?1 WHERE citizen_id = ?2",
        [&Utc::now().timestamp().to_string(), &citizen_id],
    );
    
    Json(serde_json::json!({
        "status": "ok",
        "citizen_id": citizen_id,
        "message": "Citizen marked as offline"
    }))
}

async fn cast_vote_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let vote_id = payload.get("vote_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let citizen_id = payload.get("citizen_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let choice = payload.get("choice")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    
    let db = state.db.lock().await;
    
    let _ = db.execute(
        "INSERT OR REPLACE INTO vote_choices (vote_id, citizen_id, choice, voted_at) VALUES (?1, ?2, ?3, ?4)",
        [vote_id, citizen_id, choice, &Utc::now().timestamp().to_string()],
    );
    
    Json(serde_json::json!({
        "status": "ok",
        "message": format!("Vote cast for {}", choice)
    }))
}

async fn add_peer_to_network(
    State(state): State<Arc<AppState>>,
    Json(peer): Json<Node>,
) -> Json<serde_json::Value> {
    println!("🔵 Получен запрос /peers/network");
    println!("📝 Пир: {} ({})", peer.id, peer.url);
    
    let event = Event {
        event_id: format!("peer_add_{}", Utc::now().timestamp()),
        timestamp: Utc::now().timestamp(),
        event_type: "PeerListUpdate".to_string(),
        title: "Добавление нового узла".to_string(),
        description: format!("Добавлен узел {} ({})", peer.id, peer.url),
        initiator: state.node_id.clone(),
        data: serde_json::json!({

            "peers": [{

                "id": peer.id,

                "url": peer.url,

                "status": peer.status.to_string(),

                "version": peer.version,

                "last_seen": peer.last_seen,

            }]

        }),
        previous_hash: "0".to_string(),
        signatures: vec!["admin_sig".to_string()],
        hash: Some("".to_string()),
        public: true,
    };
    
    let mut pending = state.pending_events.lock().await;
    pending.push_back(event.clone());
    println!("✅ Событие добавлено в pending: {}", event.event_id);
    
    Json(serde_json::json!({
        "status": "ok",
        "message": "Peer will be added to network via blockchain"
    }))
}

fn init_db(path: &str) -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open(path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_number INTEGER UNIQUE NOT NULL,
            block_hash TEXT UNIQUE NOT NULL,
            previous_hash TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            block_data TEXT NOT NULL,
            events_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            initiator TEXT NOT NULL,
            data TEXT NOT NULL,
            previous_hash TEXT NOT NULL,
            signatures TEXT NOT NULL,
            hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            public TEXT DEFAULT 'true'
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            public_key TEXT,
            status TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            version TEXT NOT NULL,
            created_at TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS citizen_status (
            citizen_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS votes (
            vote_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            status TEXT NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS vote_choices (
            vote_id TEXT NOT NULL,
            citizen_id TEXT NOT NULL,
            choice TEXT,
            voted_at TEXT,
            PRIMARY KEY (vote_id, citizen_id)
        )",
        [],
    )?;
    Ok(conn)
}

async fn background_sync(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(30));
    let client = reqwest::Client::new();
    
    loop {
        interval.tick().await;
        println!("🔄 Running background sync...");
        
        let registry = state.node_registry.lock().await;
        let peers = registry.get_all_nodes().unwrap_or_default();
        let my_id = state.node_id.clone();
        let db = state.db.clone();
        
        drop(registry);
        
        for peer in peers {
            if peer.id == my_id { continue; }
            
            println!("📡 Fetching blocks from peer: {} ({})", peer.id, peer.url);
            match client.get(format!("{}/blocks", peer.url)).timeout(Duration::from_secs(5)).send().await {
                Ok(response) => {
                    if let Ok(blocks) = response.json::<Vec<Block>>().await {
                        let db_guard = db.lock().await;
                        for block in blocks {
                            let exists: bool = db_guard.query_row(
                                "SELECT COUNT(*) FROM blocks WHERE block_number = ?1",
                                [&block.block_number.to_string()],
                                |row| row.get(0)
                            ).unwrap_or(0) > 0;
                            
                            if !exists {
                                let block_json = serde_json::to_string(&block).unwrap();
                                if let Err(e) = db_guard.execute(
                                    "INSERT INTO blocks (block_number, block_hash, previous_hash, timestamp, block_data, events_count, created_at)
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                    [
                                        &block.block_number.to_string(),
                                        &block.block_hash,
                                        &block.previous_hash,
                                        &block.timestamp.to_string(),
                                        &block_json,
                                        &block.events_count.to_string(),
                                        &Utc::now().to_rfc3339(),
                                    ],
                                ) {
                                    println!("❌ Failed to insert block: {}", e);
                                } else {
                                    println!("✅ Synced block #{} from peer {}", block.block_number, peer.id);
                                    
                                    for event in block.events {
                                        let data_json = serde_json::to_string(&event.data).unwrap();
                                        let sig_json = serde_json::to_string(&event.signatures).unwrap();
                                        let _ = db_guard.execute(
                                            "INSERT OR IGNORE INTO events (event_id, timestamp, event_type, title, description, initiator, data, previous_hash, signatures, hash, created_at, public)
                                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                                            [
                                                &event.event_id, &event.timestamp.to_string(), &event.event_type,
                                                &event.title, &event.description, &event.initiator, &data_json,
                                                &event.previous_hash, &sig_json, &event.hash.as_ref().unwrap_or(&"".to_string()),
                                                &Utc::now().to_rfc3339(), &event.public.to_string(),
                                            ],
                                        );
                                    }
                                }
                            }
                        }
                    }
                },
                Err(e) => println!("❌ Failed to connect to {}: {}", peer.url, e),
            }
        }
        
        let pending = state.pending_events.lock().await.len();
        if pending > 0 {
            println!("📦 Creating block from {} pending events", pending);
            let _ = create_block(state.clone()).await;
        }
    }
}

#[tokio::main]
async fn main() {
    let db_path = std::env::var("QUAZAR_DB_PATH").unwrap_or_else(|_| "/data/quazar.db".to_string());
    let node_id = std::env::var("QUAZAR_NODE_ID").unwrap_or_else(|_| "QZ-NODE".to_string());
    let node_url = std::env::var("QUAZAR_NODE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let port = std::env::var("QUAZAR_PORT").unwrap_or_else(|_| "8080".to_string());
    
    println!("🌟 Quazar Blockchain v0.7.0 with Feed (pending only) starting...");
    
    let conn = init_db(&db_path).expect("DB init failed");
    let node_registry = Arc::new(Mutex::new(NodeRegistry::new(Connection::open(&db_path).unwrap())));
    
    let my_node = Node {
        id: node_id.clone(), url: node_url.clone(), public_key: None,
        status: NodeStatus::Alive, last_seen: Utc::now().to_rfc3339(), version: "0.7.0".to_string(),
    };
    {
        let registry = node_registry.lock().await;
        let _ = registry.upsert_node(&my_node);
    }
    
    let sync_manager = Arc::new(SyncManager::new(
        node_registry.clone(),
        node_url.clone(),
        node_id.clone(),
    ));
    
    let state = Arc::new(AppState {
        db: Arc::new(Mutex::new(conn)),
        keystore: Arc::new(Mutex::new(KeyStore::new())),
        node_registry,
        sync_manager,
        node_id,
        node_url,
        pending_events: Arc::new(Mutex::new(VecDeque::new())),
    });
    
    let state_clone = state.clone();
    tokio::spawn(async move {
        background_sync(state_clone).await;
    });
    
    let app = Router::new()
        .route("/status", get(status))
        .route("/blocks", get(get_blocks))
        .route("/events", get(get_events))
        .route("/event", post(add_event))
        .route("/nodes", get(get_nodes))
        .route("/peers", post(add_peer))
        .route("/peers/network", post(add_peer_to_network))
        .route("/online", post(online_handler))
        .route("/offline", post(offline_handler))
        .route("/vote", post(cast_vote_handler))
        .with_state(state);
    
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    println!("🎧 Listening on {}", addr);
    axum::serve(tokio::net::TcpListener::bind(&addr).await.unwrap(), app).await.unwrap();
}
