CREATE INDEX IF NOT EXISTS chat_messages_content_tsv
    ON chat_messages USING GIN (to_tsvector('russian', content));
