-- Candidacies: nomination and voting for elevated roles (Guardian, Judge, Aiya)

CREATE TABLE IF NOT EXISTS candidacies (
    id TEXT PRIMARY KEY,
    citizen_id TEXT NOT NULL REFERENCES citizens(id),
    target_role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Active',
    votes_for INTEGER NOT NULL DEFAULT 0,
    votes_against INTEGER NOT NULL DEFAULT 0,
    votes_abstain INTEGER NOT NULL DEFAULT 0,
    threshold INTEGER NOT NULL DEFAULT 0,
    nominator_id TEXT NOT NULL REFERENCES citizens(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    approved_at TIMESTAMPTZ,
    appointed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS candidacy_votes (
    id TEXT PRIMARY KEY,
    candidacy_id TEXT NOT NULL REFERENCES candidacies(id) ON DELETE CASCADE,
    citizen_id TEXT NOT NULL REFERENCES citizens(id),
    vote TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (candidacy_id, citizen_id)
);

CREATE INDEX IF NOT EXISTS idx_candidacies_status ON candidacies(status);
CREATE INDEX IF NOT EXISTS idx_candidacies_citizen_id ON candidacies(citizen_id);
CREATE INDEX IF NOT EXISTS idx_candidacies_target_role ON candidacies(target_role);
CREATE INDEX IF NOT EXISTS idx_candidacy_votes_candidacy_id ON candidacy_votes(candidacy_id);
