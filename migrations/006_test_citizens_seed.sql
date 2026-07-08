-- Seed test citizens for QUAZAR_INIT_TEST_KEYS (test_citizen_key_2026, etc.)
INSERT INTO citizens (id, name, public_key, status, role, created_at, passport_issued)
VALUES
    (
        'seed-test-citizen-001',
        'test_citizen',
        'd75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986',
        'active',
        'Citizen',
        EXTRACT(EPOCH FROM NOW())::BIGINT,
        FALSE
    ),
    (
        'seed-buyer-citizen-001',
        'buyer_citizen',
        '8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c',
        'active',
        'Citizen',
        EXTRACT(EPOCH FROM NOW())::BIGINT,
        FALSE
    ),
    (
        'seed-seller-citizen-001',
        'seller_citizen',
        'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef12345678',
        'active',
        'Citizen',
        EXTRACT(EPOCH FROM NOW())::BIGINT,
        FALSE
    )
ON CONFLICT (name) DO NOTHING;
