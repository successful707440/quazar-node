# Quazar Registry

Блокчейн-реестр на Rust (Axum + PostgreSQL) с P2P-синхронизацией блоков и распределённым block producer.

## Быстрый старт

```bash
cp .env.example .env
docker-compose up -d
```

На системах с Docker Compose V2 (plugin) можно использовать `docker compose up -d`.

Или через example-файл:

```bash
docker-compose -f docker-compose.example.yml up -d
```

Или локально:

```bash
# PostgreSQL должен быть доступен по DATABASE_URL
cargo run
```

Проверка:

```bash
curl http://localhost:8080/status
```

## Переменные окружения

| Переменная | Описание |
|------------|----------|
| `QUAZAR_MASTER_KEY` | Мастер API-ключ (обязателен) |
| `QUAZAR_NODE_SECRET` | Секрет для P2P: `GET /events`, `GET /blocks`, `POST /events/gossip` |
| `QUAZAR_REG_SECRET` | Секрет для `reg_sig_*` (CitizenAdded); **не равен** node secret |
| `DATABASE_URL` | PostgreSQL connection string |
| `QUAZAR_NODE_ID` | ID узла |
| `QUAZAR_NODE_URL` | Публичный URL узла |
| `QUAZAR_BOOTSTRAP_PEERS` | Пиры при старте: `id@url,id2@url2` |
| `QUAZAR_BLOCK_MIN_EVENTS` | Минимум событий для блока (default: 3) |
| `QUAZAR_BLOCK_MAX_WAIT_SECS` | Макс. ожидание перед блоком (default: 30) |
| `QUAZAR_RATE_LIMIT_RPS` | Rate limit на защищённых роутах (default: 60) |
| `QUAZAR_CORS_ORIGINS` | CORS origins (через запятую); `*` = permissive; без значения = dev defaults |
| `RUST_LOG` | Уровень логов (`info`, `debug`) |
| `QUAZAR_STRICT_SECRETS` | `true` — ошибка при дефолтных секретах |
| `QUAZAR_DISABLE_MASTER_KEY` | `true` — отключить master key |
| `QUAZAR_INIT_TEST_KEYS` | `true` — синхронизировать тестовые ключи в PostgreSQL |

Подписи событий:
- `ed25519_<128 hex>` — подпись hash события приватным ключом (POST /event)
- `reg_sig_*` — серверная подпись CitizenAdded (секрет `QUAZAR_REG_SECRET`, не node secret)
- `node_sig_*` — серверная подпись PeerListUpdate

`public_key` гражданина — Ed25519 публичный ключ, 64 hex-символа.

Все HTTP-ответы используют формат:

```json
{ "status": "success", "data": { ... } }
{ "status": "error", "error": "..." }
```

P2P-эндпоинты (`/events`, `/blocks`, `/nodes`) возвращают массивы в поле `data`.

## API

- `GET /status` — публичный healthcheck
- `POST /event` — добавить событие в pending (**Authorization: Bearer**, поле `key` в теле не нужно)
- `GET /events`, `GET /blocks`, `GET /nodes` — P2P (node secret или API key), ApiResponse
- `POST /events/gossip` — push pending event на узел (node secret)
- `/citizen/*` — реестр граждан (register, passport, status → pending-события, SQL через projection после блока)
- `/exchange/*` — биржа услуг
- `POST /keys`, `GET /keys`, `POST /keys/revoke` — управление API-ключами (Aiya/master), хранятся в PostgreSQL

Авторизация: `Authorization: Bearer <API_KEY>` или заголовок `X-API-Key`.

### POST /event

API-ключ передаётся **только в заголовке**, не в теле JSON:

```bash
curl -X POST http://localhost:8080/event \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "my_event_1",
    "timestamp": 1700000000,
    "event_type": "VoteCast",
    "title": "Vote",
    "description": "Cast vote",
    "initiator": "alice",
    "data": {"vote_id": "v1", "citizen_id": "uuid", "choice": "yes"},
    "previous_hash": "0",
    "signatures": ["ed25519_..."],
    "public": true
  }'
```

Допустимые форматы тела: плоский объект события или `{"event": { ... }}`.  
Устаревшее поле `"key"` в теле **игнорируется** (если клиент его всё ещё шлёт).

Без валидного ключа в заголовке — **401**. Node secret на этом маршруте — **403** (используйте `POST /events/gossip`).

## Миграции

Схема БД управляется через `migrations/` и применяется при старте (`sqlx::migrate!`).

Ручной запуск (опционально):

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
sqlx migrate run
```

## Тесты

```bash
cargo check
cargo test

# Smoke (PostgreSQL + старт сервера)
# Скрипт сам проверит PostgreSQL и при необходимости запустит:
#   docker-compose up -d postgres
bash scripts/smoke.sh

# Только PostgreSQL (старый Docker):
docker-compose up -d postgres

# P2P ApiResponse (сервер уже запущен)
bash scripts/test_p2p.sh
```

## CI

GitHub Actions: `cargo check`, `cargo test`, сборка и `scripts/smoke.sh` с PostgreSQL service.
