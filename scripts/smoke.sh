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
export QUAZAR_NODE_ID="${QUAZAR_NODE_ID:-QZ-NODE-2}"
export QUAZAR_BLOCK_MIN_EVENTS="${QUAZAR_BLOCK_MIN_EVENTS:-1}"
export QUAZAR_BLOCK_MAX_WAIT_SECS="${QUAZAR_BLOCK_MAX_WAIT_SECS:-5}"
export QUAZAR_FORCE_PRODUCER="${QUAZAR_FORCE_PRODUCER:-true}"
export QUAZAR_RATE_LIMIT_RPS="${QUAZAR_RATE_LIMIT_RPS:-200}"
if [ "${SMOKE_USE_RANDOM_PORT:-1}" = "1" ]; then
  export QUAZAR_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')"
fi
BASE_URL="http://127.0.0.1:${QUAZAR_PORT}"
SMOKE_NAME="smoke$(python3 -c 'import random,string; print("".join(random.choices(string.ascii_lowercase, k=8)))')"

wait_for_citizen() {
  local name="$1"
  local id=""
  for _ in $(seq 1 35); do
    id=$(curl -sf "$BASE_URL/citizen/list" \
      -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('data', {}).get('citizens', []):
    if c.get('name') == '${name}':
        print(c.get('id', ''))
        break
")
    if [ -n "$id" ]; then
      echo "$id"
      return 0
    fi
    sleep 1
  done
  echo "Citizen ${name} not found in registry within 35s" >&2
  return 1
}

ensure_registered_citizen() {
  local name="$1"
  local pubkey="$2"
  local list
  list=$(curl -sf "$BASE_URL/citizen/list" -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
  if echo "$list" | grep -qE "\"name\"[[:space:]]*:[[:space:]]*\"${name}\""; then
    return 0
  fi
  curl -sf -X POST "$BASE_URL/citizen/register" \
    -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${name}\",\"public_key\":\"${pubkey}\",\"role\":\"Citizen\",\"birth_place\":\"SmokeCity\"}" \
    | grep -q '"status":"success"'
  wait_for_citizen "$name" >/dev/null
}

issue_passport_and_wait() {
  local citizen_id="$1"
  curl -sf -X POST "$BASE_URL/citizen/${citizen_id}/passport" \
    -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
    -H "Content-Type: application/json" \
    -d '{"expires_in_days": 365}' >/dev/null
  for _ in $(seq 1 35); do
    local status
    status=$(curl -sf "$BASE_URL/citizen/${citizen_id}" \
      -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | python3 -c "
import sys, json
d = json.load(sys.stdin).get('data', {})
print(d.get('status', ''))
")
    if [ "$status" = "active" ]; then
      return 0
    fi
    sleep 1
  done
  echo "Citizen ${citizen_id} did not become active within 35s" >&2
  return 1
}

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

SMOKE_ID=$(wait_for_citizen "$SMOKE_NAME")
echo "Registered smoke citizen: ${SMOKE_ID}"

EVENTS=$(curl -sf "$BASE_URL/events" \
  -H "Authorization: Bearer $QUAZAR_NODE_SECRET")

echo "$EVENTS" | grep -q '"status":"success"'

BLOCKS=$(curl -sf "$BASE_URL/blocks" \
  -H "Authorization: Bearer $QUAZAR_NODE_SECRET")
echo "$BLOCKS" | grep -q '"status":"success"'
echo "$BLOCKS" | grep -q "$SMOKE_NAME"

KEYS=$(curl -sf "$BASE_URL/keys" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$KEYS" | grep -q '"status":"success"'
echo "$KEYS" | grep -q 'testcitizen'

SVOD=$(curl -sf "$BASE_URL/svod" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$SVOD" | grep -q '"status":"success"'
echo "$SVOD" | grep -q 'WEB_DEV'

SVOD_CAT=$(curl -sf "$BASE_URL/svod/categories" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$SVOD_CAT" | grep -q '"status":"success"'
echo "$SVOD_CAT" | grep -q 'IT'

issue_passport_and_wait "$SMOKE_ID"
echo "Smoke citizen passport issued (active)"

# Candidacy: nominate → vote (For) → appoint (if Approved)
# testcitizen/buyercitizen are seed citizens (migration 006/011), already active
TESTCITIZEN_ID=$(wait_for_citizen "testcitizen")
BUYER_ID=$(wait_for_citizen "buyercitizen")
echo "Test citizens ready (seed active)"

CANDIDATE_ID=$(wait_for_citizen "$SMOKE_NAME")
echo "Candidacy candidate: ${CANDIDATE_ID}"

NOMINATE=$(curl -sf -X POST "$BASE_URL/candidacy/nominate" \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d "{\"candidate_id\": \"${CANDIDATE_ID}\", \"target_role\": \"Guardian\"}")

echo "$NOMINATE" | grep -q '"status":"success"'

CANDIDACY_ID=$(echo "$NOMINATE" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['id'])")
echo "Candidacy id: ${CANDIDACY_ID}"

VOTE=$(curl -sf -X POST "$BASE_URL/candidacy/${CANDIDACY_ID}/vote" \
  -H "Authorization: Bearer buyer_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"vote":"For"}')

echo "$VOTE" | grep -q '"status":"success"'

CAND_STATUS=$(echo "$VOTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['status'])")
echo "Candidacy status after vote: ${CAND_STATUS}"

if [ "$CAND_STATUS" = "Approved" ]; then
  APPOINT=$(curl -sf -X POST "$BASE_URL/candidacy/${CANDIDACY_ID}/appoint" \
    -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
  echo "$APPOINT" | grep -q '"status":"success"'
  echo "$APPOINT" | grep -q '"status":"Appointed"'
  echo "Candidacy appointed"
fi

CAND_LIST=$(curl -sf "$BASE_URL/candidacy/list" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY")
echo "$CAND_LIST" | grep -q '"status":"success"'
echo "$CAND_LIST" | grep -q "$CANDIDACY_ID"

echo "Smoke test passed"
