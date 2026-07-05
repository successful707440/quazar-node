-- Legacy upgrade: nodes.last_seen TEXT → TIMESTAMPTZ (no-op if already TIMESTAMPTZ)
DO $$
BEGIN
    ALTER TABLE nodes
        ALTER COLUMN last_seen TYPE TIMESTAMPTZ
        USING last_seen::timestamptz;
EXCEPTION
    WHEN others THEN NULL;
END $$;
