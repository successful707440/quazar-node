#!/usr/bin/env bash
# Extended smoke: start server, citizen register, P2P ApiResponse, API keys in PostgreSQL
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export DATABASE_URL="${DATABASE_URL:-postgres://quazar:quazar@localhost:5432/quazar}"
export QUAZAR_MASTER_KEY="${QUAZAR_MASTER_KEY:-QUAZAR_MASTER_KEY_CI}"
export QUAZAR_NODE_SECRET="${QUAZAR_NODE_SECRET:-QUAZAR_NODE_SECRET_CI}"
export QUAZAR_REG_SECRET="${QUAZAR_REG_SECRET:-QUAZAR_REG_SECRET_CI}"
export QUAZAR_INIT_TEST_KEYS="${QUAZAR_INIT_TEST_KEYS:-true}"
export QUAZAR_PORT="${QUAZAR_PORT:-8080}"
if [ "${SMOKE_USE_RANDOM_PORT:-1}" = "1" ]; then
  export QUAZAR_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')"
fi
BASE_URL="http://127.0.0.1:${QUAZAR_PORT}"
SMOKE_NAME="smoke$(python3 -c 'import random,string; print("".join(random.choices(string.ascii_lowercase, k=8)))')"

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
if [[ "$DATABASE_URL" =~ @([^:/]+):([0-9]+)/ ]]; then
  PG_HOST="${BASH_REMATCH[1]}"
  PG_PORT="${BASH_REMATCH[2]}"
fi

COMPOSE=()

resolve_compose() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1 && docker-compose version >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
  else
    echo "Neither 'docker compose' nor 'docker-compose' found in PATH" >&2
    exit 1
  fi
}

postgres_ready() {
  if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h "$PG_HOST" -p "$PG_PORT" -U quazar -d quazar >/dev/null 2>&1
    return $?
  fi
  if command -v nc >/dev/null 2>&1; then
    nc -z "$PG_HOST" "$PG_PORT" >/dev/null 2>&1
    return $?
  fi
  python3 - <<PY >/dev/null 2>&1
import socket
s = socket.socket()
s.settimeout(1)
s.connect(("${PG_HOST}", int("${PG_PORT}")))
s.close()
PY
}

ensure_postgres() {
  if postgres_ready; then
    echo "PostgreSQL is reachable at ${PG_HOST}:${PG_PORT}"
    return 0
  fi

  if [ "${SMOKE_AUTO_START_POSTGRES:-1}" != "1" ]; then
    echo "PostgreSQL is not reachable at ${PG_HOST}:${PG_PORT}" >&2
    echo "Start it manually, for example:" >&2
    echo "  docker-compose up -d postgres" >&2
    exit 1
  fi

  resolve_compose

  local compose_file=()
  if [ -f docker-compose.yml ]; then
    compose_file=(-f docker-compose.yml)
  elif [ -f docker-compose.example.yml ]; then
    compose_file=(-f docker-compose.example.yml)
  else
    echo "PostgreSQL is not reachable and no docker-compose.yml found" >&2
    exit 1
  fi

  echo "Starting PostgreSQL via ${COMPOSE[*]} ${compose_file[*]} up -d postgres ..."
  "${COMPOSE[@]}" "${compose_file[@]}" up -d postgres
  sleep 5

  for _ in $(seq 1 30); do
    if postgres_ready; then
      echo "PostgreSQL is ready at ${PG_HOST}:${PG_PORT}"
      return 0
    fi
    sleep 1
  done

  echo "PostgreSQL did not become ready at ${PG_HOST}:${PG_PORT} within 35 seconds" >&2
  exit 1
}

ensure_postgres

cargo build --locked

BIN="${CARGO_TARGET_DIR:-target}/debug/quazar_registry"
if [ ! -x "$BIN" ]; then
  BIN="$(find "${CARGO_TARGET_DIR:-target}/debug/deps" -maxdepth 1 -type f -name 'quazar_registry-*' \
    ! -name '*.d' ! -name '*.txt' | head -1)"
fi
if [ -z "$BIN" ] || [ ! -x "$BIN" ]; then
  echo "Binary not found after build" >&2
  exit 1
fi

"$BIN" &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  if curl -sf "$BASE_URL/status" >/dev/null; then
    break
  fi
  sleep 1
done

curl -sf "$BASE_URL/status" | grep -q '"status":"success"'

REGISTER=$(curl -sf -X POST "$BASE_URL/citizen/register" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"${SMOKE_NAME}\",\"public_key\":\"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986\",\"role\":\"Citizen\",\"birth_place\":\"SmokeCity\"}")

echo "$REGISTER" | grep -q '"status":"success"'

EVENTS=$(curl -sf "$BASE_URL/events" \
  -H "Authorization: Bearer $QUAZAR_NODE_SECRET")

echo "$EVENTS" | grep -q '"status":"success"'
echo "$EVENTS" | grep -q 'CitizenAdded'
echo "$EVENTS" | grep -q "$SMOKE_NAME"

BLOCKS=$(curl -sf "$BASE_URL/blocks" \
  -H "Authorization: Bearer $QUAZAR_NODE_SECRET")
echo "$BLOCKS" | grep -q '"status":"success"'

KEYS=$(curl -sf "$BASE_URL/keys" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$KEYS" | grep -q '"status":"success"'
echo "$KEYS" | grep -q 'test_citizen'

echo "Smoke test passed"
