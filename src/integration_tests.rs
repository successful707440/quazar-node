use std::sync::atomic::{AtomicU64, Ordering};
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

/// Mirrors block_producer: events INSERT + projection + pending DELETE (projection only after block confirmation).
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

    use crate::auth::{init_master_key, init_node_secret, init_reg_secret, AuthContext};
    use crate::candidacy::{appoint_candidate, nominate_candidate, vote_for_candidate, NominateRequest, VoteRequest};
    use crate::types::Role;
    use crate::AppState;
    use crate::nodes::NodeRegistry;

    if std::env::var("QUAZAR_MASTER_KEY").is_err() {
        std::env::set_var("QUAZAR_MASTER_KEY", "integration-test-master");
    }
    if std::env::var("QUAZAR_NODE_SECRET").is_err() {
        std::env::set_var("QUAZAR_NODE_SECRET", "integration-test-node");
    }
    if std::env::var("QUAZAR_REG_SECRET").is_err() {
        std::env::set_var("QUAZAR_REG_SECRET", "integration-test-reg");
    }
    init_master_key();
    init_node_secret();
    init_reg_secret();

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
