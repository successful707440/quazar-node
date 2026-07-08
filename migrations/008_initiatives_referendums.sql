-- Citizen initiatives (laws) and referendums (elections)

CREATE TABLE IF NOT EXISTS initiatives (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Proposed',
    proposer_id TEXT NOT NULL REFERENCES citizens(id),
    votes_for INTEGER NOT NULL DEFAULT 0,
    votes_against INTEGER NOT NULL DEFAULT 0,
    votes_abstain INTEGER NOT NULL DEFAULT 0,
    threshold INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    passed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS initiative_votes (
    id TEXT PRIMARY KEY,
    initiative_id TEXT NOT NULL REFERENCES initiatives(id) ON DELETE CASCADE,
    citizen_id TEXT NOT NULL REFERENCES citizens(id),
    vote TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (initiative_id, citizen_id)
);

CREATE TABLE IF NOT EXISTS referendums (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    target_decision TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Active',
    announcer_id TEXT NOT NULL REFERENCES citizens(id),
    votes_for INTEGER NOT NULL DEFAULT 0,
    votes_against INTEGER NOT NULL DEFAULT 0,
    votes_abstain INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS referendum_votes (
    id TEXT PRIMARY KEY,
    referendum_id TEXT NOT NULL REFERENCES referendums(id) ON DELETE CASCADE,
    citizen_id TEXT NOT NULL REFERENCES citizens(id),
    vote TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (referendum_id, citizen_id)
);

CREATE INDEX IF NOT EXISTS idx_initiatives_status ON initiatives(status);
CREATE INDEX IF NOT EXISTS idx_initiatives_proposer_id ON initiatives(proposer_id);
CREATE INDEX IF NOT EXISTS idx_initiative_votes_initiative_id ON initiative_votes(initiative_id);
CREATE INDEX IF NOT EXISTS idx_referendums_status ON referendums(status);
CREATE INDEX IF NOT EXISTS idx_referendum_votes_referendum_id ON referendum_votes(referendum_id);
