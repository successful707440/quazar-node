-- Rename seed test citizens to Latin-only names (no underscores).
UPDATE citizens SET name = 'testcitizen' WHERE name = 'test_citizen';
UPDATE citizens SET name = 'buyercitizen' WHERE name = 'buyer_citizen';
UPDATE citizens SET name = 'sellercitizen' WHERE name = 'seller_citizen';

UPDATE api_keys SET citizen_name = 'testcitizen' WHERE citizen_name = 'test_citizen';
UPDATE api_keys SET citizen_name = 'buyercitizen' WHERE citizen_name = 'buyer_citizen';
UPDATE api_keys SET citizen_name = 'sellercitizen' WHERE citizen_name = 'seller_citizen';

UPDATE events
SET data = jsonb_set(data::jsonb, '{citizen_name}', '"testcitizen"')::text
WHERE event_type = 'CitizenAdded'
  AND data::jsonb->>'citizen_name' = 'test_citizen';

UPDATE events
SET data = jsonb_set(data::jsonb, '{citizen_name}', '"buyercitizen"')::text
WHERE event_type = 'CitizenAdded'
  AND data::jsonb->>'citizen_name' = 'buyer_citizen';

UPDATE events
SET data = jsonb_set(data::jsonb, '{citizen_name}', '"sellercitizen"')::text
WHERE event_type = 'CitizenAdded'
  AND data::jsonb->>'citizen_name' = 'seller_citizen';

UPDATE pending_events
SET event_data = jsonb_set(
    event_data::jsonb,
    '{data,citizen_name}',
    '"testcitizen"'
)::text
WHERE event_data::jsonb->>'event_type' = 'CitizenAdded'
  AND event_data::jsonb->'data'->>'citizen_name' = 'test_citizen';

UPDATE pending_events
SET event_data = jsonb_set(
    event_data::jsonb,
    '{data,citizen_name}',
    '"buyercitizen"'
)::text
WHERE event_data::jsonb->>'event_type' = 'CitizenAdded'
  AND event_data::jsonb->'data'->>'citizen_name' = 'buyer_citizen';

UPDATE pending_events
SET event_data = jsonb_set(
    event_data::jsonb,
    '{data,citizen_name}',
    '"sellercitizen"'
)::text
WHERE event_data::jsonb->>'event_type' = 'CitizenAdded'
  AND event_data::jsonb->'data'->>'citizen_name' = 'seller_citizen';
