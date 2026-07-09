-- Master-key auth resolves to citizen "successful" (Aiya); required for referendum/candidacy integration tests.
INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
VALUES (
    'seed-master-aiya-001',
    'successful',
    '0000000000000000000000000000000000000000000000000000000000000001',
    'active',
    'Aiya',
    EXTRACT(EPOCH FROM NOW())::BIGINT,
    FALSE
)
ON CONFLICT (name) DO NOTHING;

-- Citizen status: pending until passport issued; active after passport.
UPDATE citizens SET status = 'active' WHERE passport_issued = true;
UPDATE citizens SET status = 'pending' WHERE passport_issued = false AND name != 'successful';
