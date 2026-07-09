-- Align api_keys.role with citizens.role (source of truth for citizen permissions).
UPDATE api_keys SET role = citizens.role
FROM citizens
WHERE api_keys.citizen_name = citizens.name
  AND api_keys.role IS DISTINCT FROM citizens.role;
