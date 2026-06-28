#!/bin/bash

echo "=== Тестирование модуля биржи Quazar ==="
echo

BASE_URL="http://localhost:8080"
API_KEY="QUAZAR_MASTER_KEY_2026"

echo "1. Добавляем баланс для пользователя test_citizen (Aiya)"
curl -s -X POST "$BASE_URL/exchange/balance/add" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"citizen_id": "test_citizen", "amount": 1000}' | jq '.'
echo

echo "2. Создаем предложение от test_citizen"
curl -s -X POST "$BASE_URL/exchange/offer" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"service": "Web Development", "price": 100, "quantity": 5}' | jq '.'
echo

echo "3. Получаем все активные предложения"
curl -s -H "X-API-Key: $API_KEY" \
  "$BASE_URL/exchange/offers" | jq '.'
echo

echo "4. Проверяем баланс test_citizen"
curl -s -H "X-API-Key: $API_KEY" \
  "$BASE_URL/exchange/balance" | jq '.'
echo

echo "5. Создаем заказ (покупаем)"
# Получаем ID предложения
OFFER_ID=$(curl -s -H "X-API-Key: $API_KEY" "$BASE_URL/exchange/offers" | jq -r '.data[0].id')
if [ "$OFFER_ID" != "null" ] && [ -n "$OFFER_ID" ]; then
  echo "Найден offer_id: $OFFER_ID"
  curl -s -X POST "$BASE_URL/exchange/order" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"offer_id\": \"$OFFER_ID\", \"quantity\": 1}" | jq '.'
else
  echo "Нет активных предложений для покупки"
fi
echo

echo "6. Проверяем баланс после покупки"
curl -s -H "X-API-Key: $API_KEY" \
  "$BASE_URL/exchange/balance" | jq '.'
echo

echo "7. Получаем заказы пользователя"
curl -s -H "X-API-Key: $API_KEY" \
  "$BASE_URL/exchange/orders" | jq '.'
echo

echo "=== Тестирование завершено ==="
