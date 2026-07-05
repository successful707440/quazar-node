#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ -f "$ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

export DATABASE_URL="${DATABASE_URL:-postgres://quazar:quazar@localhost:5432/quazar}"
export QUAZAR_MASTER_KEY="${QUAZAR_MASTER_KEY:-QUAZAR_MASTER_KEY_CI}"
export QUAZAR_NODE_SECRET="${QUAZAR_NODE_SECRET:-QUAZAR_NODE_SECRET_CI}"
export QUAZAR_REG_SECRET="${QUAZAR_REG_SECRET:-QUAZAR_REG_SECRET_CI}"
export QUAZAR_PORT="${QUAZAR_PORT:-8080}"
BASE_URL="http://127.0.0.1:${QUAZAR_PORT}"

echo "=== P2P / ApiResponse smoke ==="

STATUS=$(curl -sf "$BASE_URL/status")
echo "$STATUS" | grep -q '"status":"success"'
echo "OK: public /status"

EVENTS=$(curl -sf "$BASE_URL/events" -H "Authorization: Bearer $QUAZAR_NODE_SECRET")
echo "$EVENTS" | grep -q '"status":"success"'
echo "$EVENTS" | grep -q '"data"'
echo "OK: GET /events ApiResponse"

BLOCKS=$(curl -sf "$BASE_URL/blocks" -H "Authorization: Bearer $QUAZAR_NODE_SECRET")
echo "$BLOCKS" | grep -q '"status":"success"'
echo "$BLOCKS" | grep -q '"data"'
echo "OK: GET /blocks ApiResponse"

NODES=$(curl -sf "$BASE_URL/nodes" -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$NODES" | grep -q '"status":"success"'
echo "OK: GET /nodes ApiResponse"

echo "=== P2P tests passed (server must already be running on $BASE_URL) ==="
