use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::Duration;

use sqlx::{PgPool, Postgres, Transaction};

use crate::blockchain::{
    build_citizen_added_event, build_passport_issued_event, build_passport_revoked_event,
    build_citizen_suspended_event,
};
use crate::db;
use crate::models::Event;
use crate::pending;
use crate::projection::apply_event_projections_in_tx;

static TEST_AUTH_INIT: Once = Once::new();

fn ensure_test_auth_secrets() {
    TEST_AUTH_INIT.call_once(|| {
        if std::env::var("QUAZAR_MASTER_KEY").is_err() {
            std::env::set_var("QUAZAR_MASTER_KEY", "integration-test-master");
        }
        if std::env::var("QUAZAR_NODE_SECRET").is_err() {
            std::env::set_var("QUAZAR_NODE_SECRET", "integration-test-node");
        }
        if std::env::var("QUAZAR_REG_SECRET").is_err() {
            std::env::set_var("QUAZAR_REG_SECRET", "integration-test-reg");
        }
        crate::auth::init_master_key();
        crate::auth::init_node_secret();
        crate::auth::init_reg_secret();
    });
}

async fn postgres_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://quazar:quazar@localhost:5432/quazar".to_string());
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(2))
        .connect(&url)
        .await
        .ok()
}

/// Unique ids per test run — avoids pollution when a prior run or live server already projected a fixed citizen.
fn unique_citizen_fixture() -> (String, String, String) {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let citizen_id = format!("citizen-test-{}-{}", n, uuid::Uuid::new_v4());
    let event_id = format!("citizen_add_{}", citizen_id);
    let name = format!("pend{}_{}", n, uuid::Uuid::new_v4().simple());
    (event_id, citizen_id, name)
}

async fn citizen_exists(pool: &PgPool, citizen_id: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM citizens WHERE id = $1)")
        .bind(citizen_id)
        .fetch_one(pool)
        .await
        .unwrap_or(false)
}

async fn pending_exists(pool: &PgPool, event_id: &str) -> bool {
    pending::exists(pool, event_id).await.unwrap_or(false)
}

async fn confirm_event_in_block_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let data_json = serde_json::to_string(&event.data).map_err(|e| e.to_string())?;
    let sig_json = serde_json::to_string(&event.signatures).map_err(|e| e.to_string())?;
    sqlx::query(
        "INSERT INTO events (event_id, timestamp, event_type, title, description, initiator, data, previous_hash, signatures, hash, created_at, public)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(&event.event_id)
    .bind(event.timestamp)
    .bind(&event.event_type)
    .bind(&event.title)
    .bind(&event.description)
    .bind(&event.initiator)
    .bind(data_json)
    .bind(&event.previous_hash)
    .bind(sig_json)
    .bind(event.hash.as_deref().unwrap_or(""))
    .bind(event.public)
    .execute(&mut **tx)
    .await
    .map_err(|e| e.to_string())?;

    apply_event_projections_in_tx(tx, std::slice::from_ref(event)).await?;
    pending::delete_in_tx(tx, std::slice::from_ref(&event.event_id)).await
}

async fn activate_citizen_with_passport(pool: &PgPool, citizen_id: &str, citizen_name: &str) {
    let passport_id = format!("passport-{citizen_id}");
    let issue_event = build_passport_issued_event(
        &format!("passport_issue_{passport_id}"),
        &passport_id,
        citizen_id,
        citizen_name,
        1_700_000_050,
        1_800_000_050,
        "system",
        1_700_000_050,
    );
    let mut tx = pool.begin().await.expect("begin passport tx");
    confirm_event_in_block_tx(&mut tx, &issue_event)
        .await
        .expect("passport projection");
    tx.commit().await.expect("commit passport tx");
}

#[tokio::test]
async fn citizen_added_projection_writes_citizen() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip citizen_added_projection_writes_citizen: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let event = build_citizen_added_event(
        "citizen_add_test_alice",
        "citizen-uuid-alice",
        "alice",
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986",
        "TestCity",
        "Citizen",
        "system",
        1_700_000_000,
    );

    let mut tx = pool.begin().await.expect("begin tx");
    apply_event_projections_in_tx(&mut tx, std::slice::from_ref(&event))
        .await
        .expect("projection should succeed");
    tx.commit().await.expect("commit tx");

    let name: Option<String> = sqlx::query_scalar("SELECT name FROM citizens WHERE id = $1")
        .bind("citizen-uuid-alice")
        .fetch_optional(&pool)
        .await
        .expect("query citizens");

    assert_eq!(name.as_deref(), Some("alice"));

    let status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind("citizen-uuid-alice")
        .fetch_one(&pool)
        .await
        .expect("citizen status");
    assert_eq!(status, "pending");
}

#[tokio::test]
async fn pending_registration_not_visible_in_citizens() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip pending_registration_not_visible_in_citizens: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let (event_id, citizen_id, name) = unique_citizen_fixture();
    let event = build_citizen_added_event(
        &event_id,
        &citizen_id,
        &name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "PendingCity",
        "Citizen",
        "system",
        1_700_000_001,
    );

    let inserted = pending::insert(&pool, &event)
        .await
        .expect("insert pending");
    assert_eq!(inserted, pending::PendingInsertResult::Inserted);

    assert!(
        pending_exists(&pool, &event_id).await,
        "event must stay in pending_events before block"
    );
    assert!(
        !citizen_exists(&pool, &citizen_id).await,
        "citizen must not appear in SQL while event is only pending (no projection yet)"
    );

    let mut tx = pool.begin().await.expect("begin tx");
    confirm_event_in_block_tx(&mut tx, &event)
        .await
        .expect("block confirmation should project citizen");
    tx.commit().await.expect("commit block tx");

    assert!(
        citizen_exists(&pool, &citizen_id).await,
        "citizen must appear in SQL after block confirmation + projection"
    );
    let status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(&citizen_id)
        .fetch_one(&pool)
        .await
        .expect("status after registration");
    assert_eq!(status, "pending");
    assert!(
        !pending_exists(&pool, &event_id).await,
        "pending row must be removed after block"
    );
}

#[tokio::test]
async fn vote_lifecycle_active_and_finalize() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip vote_lifecycle_active_and_finalize: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let vote_id = "vote_integration_test";
    let start = chrono::Utc::now();
    let end = start + chrono::Duration::hours(1);

    sqlx::query(
        "INSERT INTO votes (vote_id, title, description, start_time, end_time, status)
         VALUES ($1, $2, $3, $4, $5, 'active')
         ON CONFLICT (vote_id) DO UPDATE SET status = EXCLUDED.status, end_time = EXCLUDED.end_time",
    )
    .bind(vote_id)
    .bind("Integration vote")
    .bind("Test vote lifecycle")
    .bind(start)
    .bind(end)
    .execute(&pool)
    .await
    .expect("insert vote");

    assert!(crate::votes::vote_is_active(&pool, vote_id)
        .await
        .expect("check active vote"));

    sqlx::query("UPDATE votes SET status = 'finalized' WHERE vote_id = $1")
        .bind(vote_id)
        .execute(&pool)
        .await
        .expect("finalize vote");

    assert!(!crate::votes::vote_is_active(&pool, vote_id)
        .await
        .expect("check finalized vote"));
}

#[tokio::test]
async fn passport_issue_and_revoke_projection() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip passport_issue_and_revoke_projection: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let citizen_id = "citizen-passport-test";
    let add_event = build_citizen_added_event(
        "citizen_add_passport_test",
        citizen_id,
        "carol",
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986",
        "TestCity",
        "Citizen",
        "system",
        1_700_000_010,
    );
    let passport_id = "passport-test-id";
    let issue_event = build_passport_issued_event(
        "passport_issue_test",
        passport_id,
        citizen_id,
        "carol",
        1_700_000_011,
        1_800_000_011,
        "system",
        1_700_000_011,
    );
    let revoke_event = build_passport_revoked_event(
        "passport_revoke_test",
        passport_id,
        citizen_id,
        "carol",
        "system",
        1_700_000_012,
    );

    let mut tx = pool.begin().await.expect("begin tx");
    apply_event_projections_in_tx(&mut tx, &[add_event, issue_event])
        .await
        .expect("issue projection");
    tx.commit().await.expect("commit issue");

    let issued: bool = sqlx::query_scalar(
        "SELECT passport_issued FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_one(&pool)
    .await
    .expect("passport_issued flag");
    assert!(issued);

    let status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(citizen_id)
        .fetch_one(&pool)
        .await
        .expect("status after passport");
    assert_eq!(status, "active");

    let mut tx = pool.begin().await.expect("begin tx");
    apply_event_projections_in_tx(&mut tx, std::slice::from_ref(&revoke_event))
        .await
        .expect("revoke projection");
    tx.commit().await.expect("commit revoke");

    let issued: bool = sqlx::query_scalar(
        "SELECT passport_issued FROM citizens WHERE id = $1",
    )
    .bind(citizen_id)
    .fetch_one(&pool)
    .await
    .expect("passport_issued after revoke");
    assert!(!issued);
}

#[tokio::test]
async fn citizen_suspended_projection() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip citizen_suspended_projection: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let citizen_id = "citizen-status-test";
    let add_event = build_citizen_added_event(
        "citizen_add_status_test",
        citizen_id,
        "dave",
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "TestCity",
        "Citizen",
        "system",
        1_700_000_020,
    );
    let suspend_event = build_citizen_suspended_event(
        "citizen_suspend_test",
        citizen_id,
        "dave",
        "system",
        1_700_000_021,
    );

    let mut tx = pool.begin().await.expect("begin tx");
    apply_event_projections_in_tx(&mut tx, &[add_event, suspend_event])
        .await
        .expect("status projection");
    tx.commit().await.expect("commit");

    let status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(citizen_id)
        .fetch_one(&pool)
        .await
        .expect("citizen status");
    assert_eq!(status, "suspended");
}

#[tokio::test]
async fn svod_catalog_and_exchange_integration() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip svod_catalog_and_exchange_integration: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let web_dev = crate::svod::get_service_by_code(&pool, "WEB_DEV", true)
        .await
        .expect("seed WEB_DEV service");
    assert_eq!(web_dev.base_price, 100);

    let catalog = crate::svod::get_catalog(&pool, true)
        .await
        .expect("catalog");
    assert!(catalog.iter().any(|s| s.code == "WEB_DEV"));

    let svc_code = format!("SMOKE_SVC_{}", uuid::Uuid::new_v4().simple());

    let custom = crate::svod::create_service(
        &pool,
        crate::svod::CreateServiceRequest {
            code: svc_code.clone(),
            name: "Smoke Service".to_string(),
            description: Some("integration test".to_string()),
            category_id: None,
            category_code: Some("IT".to_string()),
            base_price: 50,
            min_quantity: Some(1),
            max_quantity: Some(10),
        },
    )
    .await
    .expect("create service");

    assert_eq!(custom.code, svc_code);

    let err = crate::svod::create_service(
        &pool,
        crate::svod::CreateServiceRequest {
            code: svc_code.clone(),
            name: "Duplicate".to_string(),
            description: None,
            category_id: None,
            category_code: None,
            base_price: 10,
            min_quantity: None,
            max_quantity: None,
        },
    )
    .await;
    assert!(err.is_err());

    let disabled = crate::svod::toggle_service(&pool, &svc_code, false)
        .await
        .expect("disable");
    assert!(!disabled.is_active);

    let inactive = crate::svod::get_service_by_code(&pool, &svc_code, true).await;
    assert!(inactive.is_err());
}

#[tokio::test]
async fn pending_insert_is_idempotent() {
    let Some(pool) = postgres_pool().await else {
        eprintln!("skip pending_insert_is_idempotent: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool)
        .await
        .expect("migrations should apply");

    let (event_id, citizen_id, name) = unique_citizen_fixture();
    let event = build_citizen_added_event(
        &event_id,
        &citizen_id,
        &name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "PendingCity",
        "Citizen",
        "system",
        1_700_000_099,
    );

    let first = pending::insert(&pool, &event)
        .await
        .expect("first insert");
    assert_eq!(first, pending::PendingInsertResult::Inserted);

    let second = pending::insert(&pool, &event)
        .await
        .expect("duplicate insert");
    assert_eq!(second, pending::PendingInsertResult::AlreadyExists);

    assert!(
        pending_exists(&pool, &event_id).await,
        "event_id must exist exactly once in pending"
    );
}

#[tokio::test]
async fn candidacy_nomination_vote_approve_flow() {
    use std::sync::Arc;

    use crate::auth::AuthContext;
    use crate::candidacy::{appoint_candidate, nominate_candidate, vote_for_candidate, NominateRequest, VoteRequest};
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip candidacy_nomination_vote_approve_flow: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().to_string();
    let candidate_id = format!("cand-target-{n}");
    let voter_id = format!("cand-voter-{n}");
    let nominator_id = format!("cand-nom-{n}");

    for (id, name) in [
        (&candidate_id, format!("candt{n}")),
        (&voter_id, format!("candv{n}")),
        (&nominator_id, format!("candn{n}")),
    ] {
        let event = build_citizen_added_event(
            &format!("citizen_add_{id}"),
            id,
            &name,
            "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
            "CandCity",
            "Citizen",
            "system",
            1_700_001_000,
        );
        let mut tx = pool.begin().await.expect("tx");
        confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
        tx.commit().await.expect("commit");
        activate_citizen_with_passport(&pool, id, &name).await;
    }

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let nominator_auth = AuthContext::from_api_key(format!("candn{n}"), Role::Citizen);
    let row = nominate_candidate(
        state.clone(),
        &nominator_auth,
        NominateRequest {
            candidate_id: candidate_id.clone(),
            target_role: "Guardian".to_string(),
        },
    )
    .await
    .expect("nominate");

    assert_eq!(row.status, "Active");
    assert_eq!(row.target_role, "Guardian");
    assert!(row.threshold >= 1);

    let voter_auth = AuthContext::from_api_key(format!("candv{n}"), Role::Citizen);
    let mut voted = vote_for_candidate(
        state.clone(),
        &voter_auth,
        &row.id,
        VoteRequest {
            vote: "For".to_string(),
        },
    )
    .await
    .expect("vote");

    let mut extra = 0u32;
    while voted.status == "Active" && voted.votes_for < voted.threshold && extra < 10 {
        extra += 1;
        let extra_id = format!("cand-extra-{n}-{extra}");
        let extra_name = format!("candx{n}{extra}");
        let event = build_citizen_added_event(
            &format!("citizen_add_{extra_id}"),
            &extra_id,
            &extra_name,
            "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
            "CandCity",
            "Citizen",
            "system",
            1_700_001_100 + extra as i64,
        );
        let mut tx = pool.begin().await.expect("tx");
        confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm extra voter");
        tx.commit().await.expect("commit");
        activate_citizen_with_passport(&pool, &extra_id, &extra_name).await;

        let extra_auth = AuthContext::from_api_key(extra_name, Role::Citizen);
        voted = vote_for_candidate(
            state.clone(),
            &extra_auth,
            &row.id,
            VoteRequest {
                vote: "For".to_string(),
            },
        )
        .await
        .expect("extra vote");
    }

    assert!(
        voted.status == "Approved" || voted.votes_for >= voted.threshold,
        "expected approval or enough votes: {:?}",
        voted
    );

    let aiya_auth = AuthContext::master();
    let appointed = appoint_candidate(state, &aiya_auth, &row.id)
        .await
        .expect("appoint");
    assert_eq!(appointed.status, "Appointed");

    let pending_events = pending::fetch_all(&pool).await.unwrap_or_default();
    for event in pending_events {
        let mut tx = pool.begin().await.expect("tx");
        confirm_event_in_block_tx(&mut tx, &event)
            .await
            .expect("confirm appoint pending");
        tx.commit().await.expect("commit");
    }

    let role: String = sqlx::query_scalar("SELECT role FROM citizens WHERE id = $1")
        .bind(&candidate_id)
        .fetch_one(&pool)
        .await
        .expect("role");
    assert_eq!(role, "Guardian");
}

#[tokio::test]
async fn chat_send_and_list_messages() {
    use std::sync::Arc;

    use crate::auth::AuthContext;
    use crate::chat::{list_messages, send_message, ListMessagesQuery, SendMessageRequest};
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip chat_send_and_list_messages: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let citizen_id = format!("chat-citizen-{n}");
    let citizen_name = format!("chatuser{n}");

    let event = build_citizen_added_event(
        &format!("citizen_add_{citizen_id}"),
        &citizen_id,
        &citizen_name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "ChatCity",
        "Citizen",
        "system",
        1_700_002_000,
    );
    let mut tx = pool.begin().await.expect("tx");
    confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
    tx.commit().await.expect("commit");
    activate_citizen_with_passport(&pool, &citizen_id, &citizen_name).await;

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let auth = AuthContext::from_api_key(citizen_name.clone(), Role::Citizen);
    let sent = send_message(
        state.clone(),
        &auth,
        SendMessageRequest {
            content: "  Тестовое сообщение чата  ".to_string(),
        },
    )
    .await
    .expect("send");

    assert_eq!(sent.content, "Тестовое сообщение чата");
    assert_eq!(sent.citizen_id, citizen_id);
    assert_eq!(sent.citizen_name, citizen_name);

    let messages = list_messages(
        &pool,
        ListMessagesQuery {
            limit: Some(10),
            before: None,
        },
    )
    .await
    .expect("list");

    assert!(
        messages.iter().any(|m| m.id == sent.id),
        "sent message should appear in list"
    );

    let empty_send = send_message(
        state,
        &auth,
        SendMessageRequest {
            content: "   ".to_string(),
        },
    )
    .await;
    assert!(empty_send.is_err(), "empty message should be rejected");
}

#[tokio::test]
async fn chat_gossip_insert_is_idempotent() {
    use std::sync::Arc;

    use crate::chat::{insert_gossip_message, ChatMessageRow, GossipInsertResult};
    use crate::AppState;
    use crate::gossip;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip chat_gossip_insert_is_idempotent: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let citizen_id = format!("gossip-chat-{n}");
    let citizen_name = format!("gossipuser{n}");

    let event = build_citizen_added_event(
        &format!("citizen_add_{citizen_id}"),
        &citizen_id,
        &citizen_name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "GossipCity",
        "Citizen",
        "system",
        1_700_004_000,
    );
    let mut tx = pool.begin().await.expect("tx");
    confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
    tx.commit().await.expect("commit");

    let message = ChatMessageRow {
        id: uuid::Uuid::new_v4().to_string(),
        citizen_id: citizen_id.clone(),
        citizen_name: citizen_name.clone(),
        content: "gossip test".to_string(),
        created_at: chrono::Utc::now(),
    };

    assert_eq!(
        insert_gossip_message(&pool, &message).await.unwrap(),
        GossipInsertResult::Inserted
    );
    assert_eq!(
        insert_gossip_message(&pool, &message).await.unwrap(),
        GossipInsertResult::AlreadyExists
    );

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let resp = gossip::receive_gossip_chat_message(&state, message.clone())
        .await
        .expect("receive gossip");
    assert_eq!(resp.status, "success");
    let data = resp.data.as_ref().expect("data");
    assert_eq!(
        data.get("message").and_then(|v| v.as_str()),
        Some("Already exists, skipped")
    );
}

#[tokio::test]
async fn initiative_propose_and_vote_flow() {
    use std::sync::Arc;

    use crate::auth::AuthContext;
    use crate::initiative::{propose_initiative, vote_on_initiative, ProposeRequest, VoteRequest};
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip initiative_propose_and_vote_flow: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let proposer_id = format!("init-prop-{n}");
    let voter_id = format!("init-vote-{n}");
    let proposer_name = format!("initp{n}");
    let voter_name = format!("initv{n}");

    for (id, name) in [(&proposer_id, &proposer_name), (&voter_id, &voter_name)] {
        let event = build_citizen_added_event(
            &format!("citizen_add_{id}"),
            id,
            name,
            "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
            "InitCity",
            "Citizen",
            "system",
            1_700_003_000,
        );
        let mut tx = pool.begin().await.expect("tx");
        confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
        tx.commit().await.expect("commit");
        activate_citizen_with_passport(&pool, id, &name).await;
    }

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let proposer_auth = AuthContext::from_api_key(proposer_name.clone(), Role::Citizen);
    let initiative = propose_initiative(
        state.clone(),
        &proposer_auth,
        ProposeRequest {
            title: "Тестовая инициатива".to_string(),
            description: "Описание инициативы для теста".to_string(),
        },
    )
    .await
    .expect("propose");

    assert_eq!(initiative.status, "Proposed");
    assert_eq!(initiative.title, "Тестовая инициатива");
    assert!(initiative.threshold >= 1);

    let voter_auth = AuthContext::from_api_key(voter_name.clone(), Role::Citizen);
    let voted = vote_on_initiative(
        state.clone(),
        &voter_auth,
        &initiative.id,
        VoteRequest {
            vote: "For".to_string(),
        },
    )
    .await
    .expect("vote");

    assert!(voted.votes_for >= 1);
}

#[tokio::test]
async fn referendum_announce_and_vote_flow() {
    use std::sync::Arc;

    use crate::auth::AuthContext;
    use crate::referendum::{announce_referendum, vote_on_referendum, AnnounceRequest, VoteRequest};
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip referendum_announce_and_vote_flow: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let voter_id = format!("ref-vote-{n}");
    let voter_name = format!("refv{n}");

    let voter_event = build_citizen_added_event(
        &format!("citizen_add_{voter_id}"),
        &voter_id,
        &voter_name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "RefCity",
        "Citizen",
        "system",
        1_700_004_000,
    );
    let mut tx = pool.begin().await.expect("tx");
    confirm_event_in_block_tx(&mut tx, &voter_event).await.expect("confirm");
    tx.commit().await.expect("commit");
    activate_citizen_with_passport(&pool, &voter_id, &voter_name).await;

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let aiya_auth = AuthContext::master();
    let referendum = announce_referendum(
        state.clone(),
        &aiya_auth,
        AnnounceRequest {
            title: "Референдум о налогах".to_string(),
            description: "Отмена решения о налогах".to_string(),
            target_decision: "Закон о налогах".to_string(),
        },
    )
    .await
    .expect("announce");

    assert_eq!(referendum.status, "Active");
    assert_eq!(referendum.target_decision, "Закон о налогах");

    let voter_auth = AuthContext::from_api_key(voter_name.clone(), Role::Citizen);
    let voted = vote_on_referendum(
        state.clone(),
        &voter_auth,
        &referendum.id,
        VoteRequest {
            vote: "Against".to_string(),
        },
    )
    .await
    .expect("vote");

    assert_eq!(voted.votes_against, 1);
}

#[tokio::test]
async fn password_login_flow() {
    use std::sync::Arc;

    use axum::extract::{Extension, Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use crate::auth::{AuthContext, KeyStore};
    use crate::auth_login::{check_password_handler, login_handler, set_password_handler, SetPasswordRequest};
    use crate::auth_login::LoginRequest;
    use crate::response::ApiResponse;
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip password_login_flow: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let citizen_id = format!("pwd-citizen-{n}");
    let citizen_name = format!("pwduser{n}");
    let api_key = format!("pwd_key_{n}");

    let event = build_citizen_added_event(
        &format!("citizen_add_{citizen_id}"),
        &citizen_id,
        &citizen_name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "PwdCity",
        "Citizen",
        "system",
        1_700_005_000,
    );
    let mut tx = pool.begin().await.expect("tx");
    confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
    tx.commit().await.expect("commit");
    activate_citizen_with_passport(&pool, &citizen_id, &citizen_name).await;

    KeyStore::upsert_key(&pool, &api_key, Role::Citizen, &citizen_name)
        .await
        .expect("upsert api key");

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let check_before = check_password_handler(
        State(state.clone()),
        Query(crate::auth_login::CheckPasswordQuery {
            name: citizen_name.clone(),
        }),
    )
    .await
    .into_response();
    assert_eq!(check_before.status(), StatusCode::OK);
    let check_body: ApiResponse = serde_json::from_slice(
        &axum::body::to_bytes(check_before.into_body(), usize::MAX)
            .await
            .expect("check body"),
    )
    .expect("check json");
    assert_eq!(
        check_body.data.as_ref().and_then(|d| d.get("has_password")).and_then(|v| v.as_bool()),
        Some(false)
    );

    let auth = AuthContext::from_api_key(citizen_name.clone(), Role::Citizen);
    let set_resp = set_password_handler(
        Extension(auth),
        State(state.clone()),
        axum::Json(SetPasswordRequest {
            password: "testpass123".to_string(),
        }),
    )
    .await
    .into_response();
    assert_eq!(set_resp.status(), StatusCode::OK);

    let check_after = check_password_handler(
        State(state.clone()),
        Query(crate::auth_login::CheckPasswordQuery {
            name: citizen_name.clone(),
        }),
    )
    .await
    .into_response();
    assert_eq!(check_after.status(), StatusCode::OK);
    let check_after_body: ApiResponse = serde_json::from_slice(
        &axum::body::to_bytes(check_after.into_body(), usize::MAX)
            .await
            .expect("check after body"),
    )
    .expect("check after json");
    assert_eq!(
        check_after_body
            .data
            .as_ref()
            .and_then(|d| d.get("has_password"))
            .and_then(|v| v.as_bool()),
        Some(true)
    );

    let wrong_login = login_handler(
        State(state.clone()),
        axum::Json(LoginRequest {
            name: citizen_name.clone(),
            password: "wrong".to_string(),
        }),
    )
    .await
    .into_response();
    assert_eq!(wrong_login.status(), StatusCode::BAD_REQUEST);

    let login_resp = login_handler(
        State(state.clone()),
        axum::Json(LoginRequest {
            name: citizen_name.clone(),
            password: "testpass123".to_string(),
        }),
    )
    .await
    .into_response();
    assert_eq!(login_resp.status(), StatusCode::OK);

    let login_body: ApiResponse = serde_json::from_slice(
        &axum::body::to_bytes(login_resp.into_body(), usize::MAX)
            .await
            .expect("login body"),
    )
    .expect("login json");
    assert_eq!(login_body.status, "success");
    let data = login_body.data.expect("login data");
    assert_eq!(data.get("citizen_id").and_then(|v| v.as_str()), Some(citizen_id.as_str()));
    assert_eq!(data.get("name").and_then(|v| v.as_str()), Some(citizen_name.as_str()));
    assert_eq!(data.get("api_key").and_then(|v| v.as_str()), Some(api_key.as_str()));
}

#[tokio::test]
async fn pending_citizen_login_blocked_until_passport() {
    use std::sync::Arc;

    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use crate::auth_login::{hash_password, login_handler, LoginRequest};
    use crate::response::ApiResponse;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    ensure_test_auth_secrets();

    let Some(pool) = postgres_pool().await else {
        eprintln!("skip pending_citizen_login_blocked_until_passport: PostgreSQL unavailable");
        return;
    };
    db::run_migrations(&pool).await.expect("migrations");

    let n = uuid::Uuid::new_v4().simple().to_string();
    let citizen_id = format!("pend-login-{n}");
    let citizen_name = format!("pendlogin{n}");

    let event = build_citizen_added_event(
        &format!("citizen_add_{citizen_id}"),
        &citizen_id,
        &citizen_name,
        "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
        "PendingCity",
        "Citizen",
        "system",
        1_700_006_000,
    );
    let mut tx = pool.begin().await.expect("tx");
    confirm_event_in_block_tx(&mut tx, &event).await.expect("confirm");
    tx.commit().await.expect("commit");

    let status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(&citizen_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "pending");

    let password_hash = hash_password("secret123");
    sqlx::query(
        r#"
        INSERT INTO citizen_credentials (citizen_id, password_hash, created_at, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        "#,
    )
    .bind(&citizen_id)
    .bind(&password_hash)
    .execute(&pool)
    .await
    .expect("insert password");

    let state = Arc::new(AppState {
        db: pool.clone(),
        node_registry: Arc::new(NodeRegistry::new(pool.clone())),
        node_id: "TEST-NODE".to_string(),
    });

    let pending_login = login_handler(
        State(state.clone()),
        axum::Json(LoginRequest {
            name: citizen_name.clone(),
            password: "secret123".to_string(),
        }),
    )
    .await
    .into_response();
    assert_eq!(pending_login.status(), StatusCode::BAD_REQUEST);
    let pending_body: ApiResponse = serde_json::from_slice(
        &axum::body::to_bytes(pending_login.into_body(), usize::MAX)
            .await
            .expect("pending login body"),
    )
    .expect("pending login json");
    assert_eq!(pending_body.status, "error");
    assert_eq!(
        pending_body.error.as_deref(),
        Some("Вход недоступен: паспорт ещё не выдан (статус pending). Обратитесь к регистратору.")
    );

    activate_citizen_with_passport(&pool, &citizen_id, &citizen_name).await;

    let active_status: String = sqlx::query_scalar("SELECT status FROM citizens WHERE id = $1")
        .bind(&citizen_id)
        .fetch_one(&pool)
        .await
        .expect("active status");
    assert_eq!(active_status, "active");

    let login_resp = login_handler(
        State(state),
        axum::Json(LoginRequest {
            name: citizen_name,
            password: "secret123".to_string(),
        }),
    )
    .await
    .into_response();
    assert_eq!(login_resp.status(), StatusCode::OK);
    let login_body: ApiResponse = serde_json::from_slice(
        &axum::body::to_bytes(login_resp.into_body(), usize::MAX)
            .await
            .expect("login body"),
    )
    .expect("login json");
    assert_eq!(login_body.status, "success");
}
