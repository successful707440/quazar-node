#!/usr/bin/env bash
# Тест Telegram-бота: регистрация гражданина + события → новый блок → алерт watcher
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi
if [[ -f "$ROOT/agent/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/agent/.env"
  set +a
fi

export QUAZAR_MASTER_KEY="${QUAZAR_MASTER_KEY:-QUAZAR_MASTER_KEY_2026}"
export QUAZAR_NODE_SECRET="${QUAZAR_NODE_SECRET:-QUAZAR_NODE_SECRET_2026}"
BASE_URL="${QUAZAR_TEST_URL:-http://127.0.0.1:8080}"

TS=$(date +%s)
TEST_NAME="bot$(python3 -c 'import random,string; print("".join(random.choices(string.ascii_lowercase, k=8)))')"
PUBKEY="d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986"

echo "=== Тест уведомления бота ==="
echo "Узел: $BASE_URL"

BLOCKS_BEFORE=$(curl -sf "$BASE_URL/status" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['blocks'])")
echo "Блоков до: $BLOCKS_BEFORE"

register_citizen() {
  local name="$1"
  curl -sf -X POST "$BASE_URL/citizen/register" \
    -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${name}\",\"public_key\":\"${PUBKEY}\",\"role\":\"Citizen\",\"birth_place\":\"BotTestCity\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['status']=='success', d; print('  OK:', d['data'].get('event_id', d))"
}

echo "1) Регистрация 3 граждан (CitizenAdded, min_events=3)..."
register_citizen "$TEST_NAME"
register_citizen "$(python3 -c 'import random,string; print("bot"+"".join(random.choices(string.ascii_lowercase,k=8)))')"
register_citizen "$(python3 -c 'import random,string; print("bot"+"".join(random.choices(string.ascii_lowercase,k=8)))')"

echo "2) Ожидание нового блока (до 45 с)..."
for _ in $(seq 1 45); do
  BLOCKS_NOW=$(curl -sf "$BASE_URL/status" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['blocks'])")
  if [[ "$BLOCKS_NOW" -gt "$BLOCKS_BEFORE" ]]; then
    echo "   Новый блок: #$BLOCKS_NOW"
    break
  fi
  sleep 1
done

if [[ "${BLOCKS_NOW:-0}" -le "$BLOCKS_BEFORE" ]]; then
  echo "Блок не создан за 45 с. Проверьте pending:" >&2
  curl -sf "$BASE_URL/events" -H "Authorization: Bearer $QUAZAR_NODE_SECRET" | python3 -m json.tool | head -30
  exit 1
fi

echo "3) Запуск watcher (--once) для отправки в Telegram..."
bash "$ROOT/scripts/run_watcher.sh" --once

echo "=== Готово. Проверьте Telegram @Quazar_Agent_Bot ==="
