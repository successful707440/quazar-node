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
echo

echo "1. Регистрация гражданина alice"
curl -s -X POST $BASE_URL/citizen/register \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "alice", "public_key": "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986", "role": "Citizen", "birth_place": "TestCity"}' | jq '.'

echo -e "\n"

echo "1b. Ожидание подтверждения alice в блокчейне (до 35 сек)"
ALICE_ID=""
for _ in $(seq 1 35); do
  ALICE_ID=$(curl -s "$BASE_URL/citizen/list" -H "Authorization: Bearer $API_KEY" | jq -r '.data.citizens[]? | select(.name=="alice") | .id')
  if [ -n "$ALICE_ID" ] && [ "$ALICE_ID" != "null" ]; then
    echo "alice подтверждена: $ALICE_ID"
    break
  fi
  sleep 1
done
if [ -z "$ALICE_ID" ] || [ "$ALICE_ID" = "null" ]; then
  echo "Ошибка: alice не появилась в реестре после регистрации" >&2
  exit 1
fi

echo -e "\n"

echo "2. Регистрация гражданина bob"
curl -s -X POST $BASE_URL/citizen/register \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "bob", "public_key": "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c", "role": "Guardian"}' | jq '.'

echo -e "\n"

echo "3. Список всех граждан"
curl -s $BASE_URL/citizen/list \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n"

echo "4. Информация о гражданине alice"
curl -s $BASE_URL/citizen/$ALICE_ID \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n"

echo "5. Выдача паспорта для alice (pending → блок)"
curl -s -X POST $BASE_URL/citizen/$ALICE_ID/passport \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"expires_in_days": 365}' | jq '.'

echo -e "\n"

echo "5b. Ожидание подтверждения паспорта (до 35 сек)"
for _ in $(seq 1 35); do
  ISSUED=$(curl -s "$BASE_URL/citizen/$ALICE_ID" -H "Authorization: Bearer $API_KEY" | jq -r '.data.passport_issued')
  if [ "$ISSUED" = "true" ]; then
    echo "паспорт подтверждён"
    break
  fi
  sleep 1
done
if [ "$ISSUED" != "true" ]; then
  echo "Ошибка: паспорт не подтверждён после выдачи" >&2
  exit 1
fi

echo -e "\n"

echo "6. Информация о alice после выдачи паспорта"
curl -s $BASE_URL/citizen/$ALICE_ID \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n"

echo "7. Поиск граждан по запросу 'ali'"
curl -s "$BASE_URL/citizen/search?q=ali" \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n"

echo "8. Обновление статуса alice на Suspended (pending → блок)"
curl -s -X PATCH $BASE_URL/citizen/$ALICE_ID/status \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"status": "suspended"}' | jq '.'

echo -e "\n"

echo "8b. Ожидание подтверждения статуса suspended (до 35 сек)"
for _ in $(seq 1 35); do
  STATUS=$(curl -s "$BASE_URL/citizen/$ALICE_ID" -H "Authorization: Bearer $API_KEY" | jq -r '.data.status')
  if [ "$STATUS" = "suspended" ]; then
    echo "статус suspended подтверждён"
    break
  fi
  sleep 1
done

echo -e "\n"

echo "9. Аннулирование паспорта alice (pending → блок)"
curl -s -X POST $BASE_URL/citizen/$ALICE_ID/passport/revoke \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n"

echo "9b. Ожидание аннулирования паспорта (до 35 сек)"
for _ in $(seq 1 35); do
  ISSUED=$(curl -s "$BASE_URL/citizen/$ALICE_ID" -H "Authorization: Bearer $API_KEY" | jq -r '.data.passport_issued')
  if [ "$ISSUED" = "false" ]; then
    echo "аннулирование паспорта подтверждено"
    break
  fi
  sleep 1
done

echo -e "\n"

echo "10. Финальное состояние alice"
curl -s $BASE_URL/citizen/$ALICE_ID \
  -H "Authorization: Bearer $API_KEY" | jq '.'

echo -e "\n=== Тестирование завершено ==="
