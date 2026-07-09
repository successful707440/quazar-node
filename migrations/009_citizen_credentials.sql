CREATE TABLE IF NOT EXISTS citizen_credentials (
    citizen_id TEXT PRIMARY KEY REFERENCES citizens(id) ON DELETE CASCADE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_citizen_credentials_citizen_id ON citizen_credentials(citizen_id);
