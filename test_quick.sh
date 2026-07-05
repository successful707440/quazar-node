#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

API_KEY="${QUAZAR_MASTER_KEY:?QUAZAR_MASTER_KEY not set — add it to .env}"
BASE_URL="http://localhost:8080"

echo "=== Тестирование Citizen Registry ==="

echo -e "\n1. Регистрация гражданина alice"
curl -s -X POST $BASE_URL/citizen/register \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "alice", "public_key": "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986", "role": "Citizen"}' | jq '.'

echo -e "\n2. Регистрация гражданина bob"
curl -s -X POST $BASE_URL/citizen/register \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "bob", "public_key": "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c", "role": "Guardian"}' | jq '.'

echo -e "\n3. Список всех граждан"
curl -s $BASE_URL/citizen/list \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n4. Поиск граждан по имени 'ali'"
curl -s "$BASE_URL/citizen/search?q=ali" \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n=== Тестирование завершено ==="
