CREATE TABLE chat_messages (
    id TEXT PRIMARY KEY,
    citizen_id TEXT NOT NULL REFERENCES citizens(id),
    citizen_name TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chat_messages_created_at ON chat_messages(created_at DESC);
