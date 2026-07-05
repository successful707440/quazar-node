CREATE TABLE IF NOT EXISTS blocks (
    id BIGSERIAL PRIMARY KEY,
    block_number BIGINT UNIQUE NOT NULL,
    block_hash TEXT UNIQUE NOT NULL,
    previous_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    block_data TEXT NOT NULL,
    events_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,
    timestamp BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    initiator TEXT NOT NULL,
    data TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    signatures TEXT NOT NULL,
    hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    public BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS pending_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,
    event_data TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    public_key TEXT,
    status TEXT NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS citizen_status (
    citizen_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    last_seen BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS votes (
    vote_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vote_choices (
    vote_id TEXT NOT NULL,
    citizen_id TEXT NOT NULL,
    choice TEXT,
    voted_at BIGINT,
    PRIMARY KEY (vote_id, citizen_id)
);

CREATE TABLE IF NOT EXISTS offers (
    id TEXT PRIMARY KEY,
    seller TEXT NOT NULL,
    service TEXT NOT NULL,
    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    buyer TEXT NOT NULL,
    offer_id TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    total_price BIGINT NOT NULL,
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS balances (
    citizen_id TEXT PRIMARY KEY,
    amount BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS citizens (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    public_key TEXT NOT NULL,
    status TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    passport_issued BOOLEAN NOT NULL DEFAULT FALSE,
    passport_expires BIGINT
);

CREATE TABLE IF NOT EXISTS passports (
    id TEXT PRIMARY KEY,
    citizen_id TEXT NOT NULL REFERENCES citizens(id) ON DELETE CASCADE,
    issued_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_citizens_name ON citizens(name);
CREATE INDEX IF NOT EXISTS idx_citizens_status ON citizens(status);
CREATE INDEX IF NOT EXISTS idx_passports_citizen_id ON passports(citizen_id);
