use sqlx::{Postgres, Transaction};

use crate::models::Event;
use crate::types::Role;

pub async fn apply_event_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    events: &[Event],
) -> Result<(), String> {
    for event in events {
        match event.event_type.as_str() {
            "CitizenAdded" => apply_citizen_added(tx, event).await?,
            "PassportIssued" => apply_passport_issued(tx, event).await?,
            "PassportRevoked" => apply_passport_revoked(tx, event).await?,
            "CitizenSuspended" => apply_citizen_status(tx, event, "suspended").await?,
            "CitizenRestored" => apply_citizen_status(tx, event, "active").await?,
            "CitizenUpdated" => apply_citizen_updated(tx, event).await?,
            "CandidateNominated" => apply_candidate_nominated(tx, event).await?,
            "CandidateVoted" => apply_candidate_voted(tx, event).await?,
            "CandidateApproved" => apply_candidate_approved(tx, event).await?,
            "CandidateAppointed" => apply_candidate_appointed(tx, event).await?,
            "LawProposed" => apply_law_proposed(tx, event).await?,
            "LawVoteStarted" => apply_law_vote_started(tx, event).await?,
            "LawVoteResult" => apply_law_vote_result(tx, event).await?,
            "VoteCast" => apply_vote_cast(tx, event).await?,
            "ElectionAnnounced" => apply_election_announced(tx, event).await?,
            _ => {}
        }
    }
    Ok(())
}

fn require_str<'a>(data: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    data.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{}: missing {}", field, field))
}

fn require_i64(data: &serde_json::Value, field: &str) -> Result<i64, String> {
    data.get(field)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("{}: missing {}", field, field))
}

async fn apply_citizen_added(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let citizen_name = require_str(&event.data, "citizen_name")?;
    let public_key = require_str(&event.data, "public_key")?;
    let role_str = event
        .data
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or("Citizen");
    let role = Role::from_str(role_str)
        .ok_or_else(|| format!("CitizenAdded: invalid role {}", role_str))?;

    let name_taken: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM citizens WHERE name = $1)",
    )
    .bind(citizen_name)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| format!("CitizenAdded name check failed for {}: {}", citizen_id, e))?;

    if name_taken {
        tracing::warn!(
            citizen_id = %citizen_id,
            name = %citizen_name,
            event_id = %event.event_id,
            "CitizenAdded skipped: name already registered"
        );
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
        VALUES ($1, $2, $3, 'active', $4, $5, FALSE)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            public_key = EXCLUDED.public_key,
            role = EXCLUDED.role
        "#,
    )
    .bind(citizen_id)
    .bind(citizen_name)
    .bind(public_key)
    .bind(role.as_str())
    .bind(event.timestamp)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CitizenAdded projection failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        name = %citizen_name,
        event_id = %event.event_id,
        "CitizenAdded projected to SQL"
    );

    Ok(())
}

async fn apply_passport_issued(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let passport_id = require_str(&event.data, "passport_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let issued_at = event
        .data
        .get("issued_at")
        .and_then(|v| v.as_i64())
        .unwrap_or(event.timestamp);
    let expires_at = require_i64(&event.data, "expires_at")?;

    sqlx::query(
        "INSERT INTO passports (id, citizen_id, issued_at, expires_at, is_valid)
         VALUES ($1, $2, $3, $4, TRUE)
         ON CONFLICT (id) DO UPDATE SET
            expires_at = EXCLUDED.expires_at,
            is_valid = TRUE",
    )
    .bind(passport_id)
    .bind(citizen_id)
    .bind(issued_at)
    .bind(expires_at)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportIssued projection failed for {}: {}", passport_id, e))?;

    sqlx::query(
        "UPDATE citizens SET passport_issued = TRUE, passport_expires = $1 WHERE id = $2",
    )
    .bind(expires_at)
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportIssued citizen update failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        passport_id = %passport_id,
        event_id = %event.event_id,
        "PassportIssued projected to SQL"
    );

    Ok(())
}

async fn apply_passport_revoked(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let passport_id = require_str(&event.data, "passport_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;

    sqlx::query("UPDATE passports SET is_valid = FALSE WHERE id = $1")
        .bind(passport_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("PassportRevoked projection failed for {}: {}", passport_id, e))?;

    sqlx::query(
        "UPDATE citizens SET passport_issued = FALSE, passport_expires = NULL WHERE id = $1",
    )
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("PassportRevoked citizen update failed for {}: {}", citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        passport_id = %passport_id,
        event_id = %event.event_id,
        "PassportRevoked projected to SQL"
    );

    Ok(())
}

async fn apply_citizen_status(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    status: &str,
) -> Result<(), String> {
    let citizen_id = require_str(&event.data, "citizen_id")?;

    sqlx::query("UPDATE citizens SET status = $1 WHERE id = $2")
        .bind(status)
        .bind(citizen_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("{} projection failed for {}: {}", event.event_type, citizen_id, e))?;

    tracing::info!(
        citizen_id = %citizen_id,
        status = %status,
        event_id = %event.event_id,
        "citizen status projected to SQL"
    );

    Ok(())
}

async fn apply_citizen_updated(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    if event.data.get("status").is_some() {
        let status = require_str(&event.data, "status")?;
        apply_citizen_status(tx, event, status).await?;
    }

    if event.data.get("role").is_some() {
        let citizen_id = require_str(&event.data, "citizen_id")?;
        let role_str = require_str(&event.data, "role")?;
        let role = Role::from_str(role_str)
            .ok_or_else(|| format!("CitizenUpdated: invalid role {}", role_str))?;

        sqlx::query("UPDATE citizens SET role = $1 WHERE id = $2")
            .bind(role.as_str())
            .bind(citizen_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| format!("CitizenUpdated role projection failed for {}: {}", citizen_id, e))?;

        sqlx::query(
            "UPDATE api_keys SET role = $1 WHERE citizen_name = (SELECT name FROM citizens WHERE id = $2)",
        )
        .bind(role.as_str())
        .bind(citizen_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("CitizenUpdated api_keys sync failed for {}: {}", citizen_id, e))?;

        tracing::info!(
            citizen_id = %citizen_id,
            role = %role.as_str(),
            event_id = %event.event_id,
            "citizen role projected to SQL"
        );
    }

    Ok(())
}

async fn apply_candidate_nominated(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let candidacy_id = require_str(&event.data, "candidacy_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let target_role = require_str(&event.data, "target_role")?;
    let nominator_id = require_str(&event.data, "nominator_id")?;
    let threshold = event
        .data
        .get("threshold")
        .and_then(|v| v.as_i64())
        .unwrap_or(1) as i32;

    sqlx::query(
        r#"
        INSERT INTO candidacies (id, citizen_id, target_role, status, votes_for, votes_against, votes_abstain, threshold, nominator_id, created_at)
        VALUES ($1, $2, $3, 'Active', 0, 0, 0, $4, $5, to_timestamp($6))
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(candidacy_id)
    .bind(citizen_id)
    .bind(target_role)
    .bind(threshold)
    .bind(nominator_id)
    .bind(event.timestamp as f64)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateNominated projection failed: {}", e))?;

    tracing::info!(candidacy_id = %candidacy_id, event_id = %event.event_id, "CandidateNominated projected");
    Ok(())
}

async fn apply_candidate_voted(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let candidacy_id = require_str(&event.data, "candidacy_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let vote = require_str(&event.data, "vote")?;
    let vote_id = format!("{}_{}", candidacy_id, citizen_id);

    sqlx::query(
        r#"
        INSERT INTO candidacy_votes (id, candidacy_id, citizen_id, vote, created_at)
        VALUES ($1, $2, $3, $4, to_timestamp($5))
        ON CONFLICT (candidacy_id, citizen_id) DO UPDATE SET vote = EXCLUDED.vote
        "#,
    )
    .bind(&vote_id)
    .bind(candidacy_id)
    .bind(citizen_id)
    .bind(vote)
    .bind(event.timestamp as f64)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateVoted projection failed: {}", e))?;

    sqlx::query(
        r#"
        UPDATE candidacies SET
            votes_for = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'For'),
            votes_against = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'Against'),
            votes_abstain = (SELECT COUNT(*)::int FROM candidacy_votes WHERE candidacy_id = $1 AND vote = 'Abstain')
        WHERE id = $1
        "#,
    )
    .bind(candidacy_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateVoted count refresh failed: {}", e))?;

    tracing::info!(candidacy_id = %candidacy_id, voter = %citizen_id, event_id = %event.event_id, "CandidateVoted projected");
    Ok(())
}

async fn apply_candidate_approved(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let candidacy_id = require_str(&event.data, "candidacy_id")?;

    sqlx::query(
        "UPDATE candidacies SET status = 'Approved', approved_at = to_timestamp($1) WHERE id = $2",
    )
    .bind(event.timestamp as f64)
    .bind(candidacy_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateApproved projection failed: {}", e))?;

    tracing::info!(candidacy_id = %candidacy_id, event_id = %event.event_id, "CandidateApproved projected");
    Ok(())
}

async fn apply_candidate_appointed(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let candidacy_id = require_str(&event.data, "candidacy_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let target_role = require_str(&event.data, "target_role")?;
    let role = Role::from_str(target_role)
        .ok_or_else(|| format!("CandidateAppointed: invalid role {}", target_role))?;

    sqlx::query(
        "UPDATE candidacies SET status = 'Appointed', appointed_at = to_timestamp($1) WHERE id = $2",
    )
    .bind(event.timestamp as f64)
    .bind(candidacy_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateAppointed candidacy update failed: {}", e))?;

    sqlx::query("UPDATE citizens SET role = $1 WHERE id = $2")
        .bind(role.as_str())
        .bind(citizen_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("CandidateAppointed role projection failed: {}", e))?;

    sqlx::query(
        "UPDATE api_keys SET role = $1 WHERE citizen_name = (SELECT name FROM citizens WHERE id = $2)",
    )
    .bind(role.as_str())
    .bind(citizen_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("CandidateAppointed api_keys sync failed: {}", e))?;

    tracing::info!(candidacy_id = %candidacy_id, citizen_id = %citizen_id, event_id = %event.event_id, "CandidateAppointed projected");
    Ok(())
}

async fn apply_law_proposed(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let law_id = require_str(&event.data, "law_id")?;
    let title = require_str(&event.data, "title")?;
    let description = require_str(&event.data, "description")?;
    let proposer_id = require_str(&event.data, "proposer_id")?;
    let threshold = event
        .data
        .get("threshold")
        .and_then(|v| v.as_i64())
        .unwrap_or(1) as i32;

    sqlx::query(
        r#"
        INSERT INTO initiatives (id, title, description, status, proposer_id, votes_for, votes_against, votes_abstain, threshold, created_at)
        VALUES ($1, $2, $3, 'Proposed', $4, 0, 0, 0, $5, to_timestamp($6))
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(law_id)
    .bind(title)
    .bind(description)
    .bind(proposer_id)
    .bind(threshold)
    .bind(event.timestamp as f64)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("LawProposed projection failed: {}", e))?;

    tracing::info!(law_id = %law_id, event_id = %event.event_id, "LawProposed projected");
    Ok(())
}

async fn apply_law_vote_started(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let law_id = require_str(&event.data, "law_id")?;
    sqlx::query(
        "UPDATE initiatives SET status = 'Proposed' WHERE id = $1 AND status != 'Passed'",
    )
    .bind(law_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("LawVoteStarted projection failed: {}", e))?;

    tracing::info!(law_id = %law_id, event_id = %event.event_id, "LawVoteStarted projected");
    Ok(())
}

async fn apply_law_vote_result(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let law_id = require_str(&event.data, "law_id")?;
    let result = event
        .data
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("Passed");

    if result == "Passed" {
        sqlx::query(
            "UPDATE initiatives SET status = 'Passed', passed_at = to_timestamp($1) WHERE id = $2",
        )
        .bind(event.timestamp as f64)
        .bind(law_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("LawVoteResult projection failed: {}", e))?;
    }

    tracing::info!(law_id = %law_id, result = %result, event_id = %event.event_id, "LawVoteResult projected");
    Ok(())
}

async fn apply_vote_cast(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let vote_id = require_str(&event.data, "vote_id")?;
    let citizen_id = require_str(&event.data, "citizen_id")?;
    let choice = require_str(&event.data, "choice")?;
    let record_id = format!("{vote_id}_{citizen_id}");

    let is_initiative: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM initiatives WHERE id = $1)",
    )
    .bind(vote_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| format!("VoteCast initiative check failed: {}", e))?;

    if is_initiative {
        sqlx::query(
            r#"
            INSERT INTO initiative_votes (id, initiative_id, citizen_id, vote, created_at)
            VALUES ($1, $2, $3, $4, to_timestamp($5))
            ON CONFLICT (initiative_id, citizen_id) DO UPDATE SET vote = EXCLUDED.vote
            "#,
        )
        .bind(&record_id)
        .bind(vote_id)
        .bind(citizen_id)
        .bind(choice)
        .bind(event.timestamp as f64)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("VoteCast initiative projection failed: {}", e))?;

        sqlx::query(
            r#"
            UPDATE initiatives SET
                votes_for = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'For'),
                votes_against = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'Against'),
                votes_abstain = (SELECT COUNT(*)::int FROM initiative_votes WHERE initiative_id = $1 AND vote = 'Abstain')
            WHERE id = $1
            "#,
        )
        .bind(vote_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("VoteCast initiative count refresh failed: {}", e))?;
    } else {
        let is_referendum: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM referendums WHERE id = $1)",
        )
        .bind(vote_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| format!("VoteCast referendum check failed: {}", e))?;

        if is_referendum {
            sqlx::query(
                r#"
                INSERT INTO referendum_votes (id, referendum_id, citizen_id, vote, created_at)
                VALUES ($1, $2, $3, $4, to_timestamp($5))
                ON CONFLICT (referendum_id, citizen_id) DO UPDATE SET vote = EXCLUDED.vote
                "#,
            )
            .bind(&record_id)
            .bind(vote_id)
            .bind(citizen_id)
            .bind(choice)
            .bind(event.timestamp as f64)
            .execute(&mut **tx)
            .await
            .map_err(|e| format!("VoteCast referendum projection failed: {}", e))?;

            sqlx::query(
                r#"
                UPDATE referendums SET
                    votes_for = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'For'),
                    votes_against = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'Against'),
                    votes_abstain = (SELECT COUNT(*)::int FROM referendum_votes WHERE referendum_id = $1 AND vote = 'Abstain')
                WHERE id = $1
                "#,
            )
            .bind(vote_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| format!("VoteCast referendum count refresh failed: {}", e))?;
        }
    }

    tracing::info!(vote_id = %vote_id, voter = %citizen_id, event_id = %event.event_id, "VoteCast projected");
    Ok(())
}

async fn apply_election_announced(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
) -> Result<(), String> {
    let election_id = require_str(&event.data, "election_id")?;
    let title = require_str(&event.data, "title")?;
    let target_decision = require_str(&event.data, "target_decision")?;
    let announcer_id = event
        .data
        .get("announcer_id")
        .and_then(|v| v.as_str())
        .unwrap_or("system");

    sqlx::query(
        r#"
        INSERT INTO referendums (id, title, description, target_decision, status, announcer_id, votes_for, votes_against, votes_abstain, created_at)
        VALUES ($1, $2, $3, $4, 'Active', $5, 0, 0, 0, to_timestamp($6))
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(election_id)
    .bind(title)
    .bind(event.description.clone())
    .bind(target_decision)
    .bind(announcer_id)
    .bind(event.timestamp as f64)
    .execute(&mut **tx)
    .await
    .map_err(|e| format!("ElectionAnnounced projection failed: {}", e))?;

    tracing::info!(election_id = %election_id, event_id = %event.event_id, "ElectionAnnounced projected");
    Ok(())
}
