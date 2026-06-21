use serde::{Serialize, Deserialize};
use chrono::Utc;
use rusqlite::{Connection, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Alive,
    Dead,
    Syncing,
}

impl ToString for NodeStatus {
    fn to_string(&self) -> String {
        match self {
            NodeStatus::Alive => "alive",
            NodeStatus::Dead => "dead",
            NodeStatus::Syncing => "syncing",
        }.to_string()
    }
}

impl From<String> for NodeStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "alive" => NodeStatus::Alive,
            "dead" => NodeStatus::Dead,
            "syncing" => NodeStatus::Syncing,
            _ => NodeStatus::Dead,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub url: String,
    pub public_key: Option<String>,
    pub status: NodeStatus,
    pub last_seen: String,
    pub version: String,
}

pub struct NodeRegistry {
    conn: Connection,
}

impl NodeRegistry {
    pub fn sync_peers(&self, peers: Vec<Node>) -> Result<()> {
        for peer in peers {
            let _ = self.upsert_node(&peer);
        }
        Ok(())
    }
    pub fn new(conn: Connection) -> Self {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                public_key TEXT,
                status TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                version TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
            [],
        ).expect("Failed to create nodes table");
        Self { conn }
    }

    pub fn upsert_node(&self, node: &Node) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes (id, url, public_key, status, last_seen, version, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE((SELECT created_at FROM nodes WHERE id = ?1), ?7))",
            [
                &node.id,
                &node.url,
                &node.public_key.clone().unwrap_or_default(),
                &node.status.to_string(),
                &node.last_seen,
                &node.version,
                &Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_alive_nodes(&self, exclude_id: &str) -> Result<Vec<Node>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, url, public_key, status, last_seen, version 
             FROM nodes 
             WHERE status = 'alive' AND id != ?1"
        )?;
        let rows = stmt.query_map([exclude_id], |row| {
            Ok(Node {
                id: row.get(0)?,
                url: row.get(1)?,
                public_key: {
                    let pk: String = row.get(2)?;
                    if pk.is_empty() { None } else { Some(pk) }
                },
                status: NodeStatus::from(row.get::<_, String>(3)?),
                last_seen: row.get(4)?,
                version: row.get(5)?,
            })
        })?;
        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row?);
        }
        Ok(nodes)
    }

    pub fn get_all_nodes(&self) -> Result<Vec<Node>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, url, public_key, status, last_seen, version FROM nodes ORDER BY created_at"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Node {
                id: row.get(0)?,
                url: row.get(1)?,
                public_key: {
                    let pk: String = row.get(2)?;
                    if pk.is_empty() { None } else { Some(pk) }
                },
                status: NodeStatus::from(row.get::<_, String>(3)?),
                last_seen: row.get(4)?,
                version: row.get(5)?,
            })
        })?;
        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row?);
        }
        Ok(nodes)
    }

    pub fn update_status(&self, node_id: &str, status: NodeStatus) -> Result<()> {
        self.conn.execute(
            "UPDATE nodes SET status = ?1, last_seen = ?2 WHERE id = ?3",
            [&status.to_string(), &Utc::now().to_rfc3339(), node_id],
        )?;
        Ok(())
    }
}

