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
- `/citizen/*` — реестр граждан (register → **pending**, passport → **active**, status → pending-события, SQL через projection после блока)
- `/candidacy/*` — выдвижение кандидатов на роли Guardian/Judge/Aiya, голосование, утверждение (5%), назначение Айей
- `/initiative/*` — гражданские инициативы (законопроекты): предложение, голосование, автоматическое принятие при 5% «За»
- `/referendum/*` — референдумы: объявление Айей, голосование граждан за/против отмены решения
- `/chat/*` — чат граждан (per-node SQL, без блокчейна)
- `/svod/*` — **Свод Оснований для Созидания** (реестр услуг/товов для биржи, валюта КВАЗИ / QZ)
- `/exchange/*` — биржа услуг (предложения только по услугам из Свода)
- `POST /keys`, `GET /keys`, `POST /keys/revoke` — управление API-ключами (Aiya/master), хранятся в PostgreSQL

Авторизация: `Authorization: Bearer <API_KEY>` или заголовок `X-API-Key`.

### Регистрация граждан и статус `pending`

Новый гражданин после подтверждения `CitizenAdded` в блоке получает статус **`pending`**. Статус **`active`** присваивается только после выдачи паспорта (`PassportIssued` в блокчейне).

Пока статус `pending`, гражданин **не может**:
- использовать API-ключ (middleware вернёт **403**);
- голосовать, выдвигать кандидатов, торговать на бирже, писать в чат.

```bash
# 1. Регистрация (ответ: registration_status = "pending")
curl -s -X POST http://localhost:8080/citizen/register \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "alice", "public_key": "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f8077986", "role": "Citizen", "birth_place": "TestCity"}' | jq .

# 2. Дождаться появления в реестре (status = "pending", passport_issued = false)
curl -s http://localhost:8080/citizen/list \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | jq '.data.citizens[] | select(.name=="alice")'

# 3. Выдача паспорта (Aiya / master)
ALICE_ID="<uuid из списка>"
curl -s -X POST "http://localhost:8080/citizen/${ALICE_ID}/passport" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"expires_in_days": 365}' | jq .

# 4. После подтверждения в блоке: status = "active", passport_issued = true
curl -s "http://localhost:8080/citizen/${ALICE_ID}" \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | jq '.data | {status, passport_issued}'
```

Ошибки для `pending`:
- **API-ключ**: «Доступ запрещён: паспорт ещё не выдан (статус pending). Дождитесь подтверждения в блокчейне.» (**403**)

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

### Кандидатуры на роли (`/candidacy`)

Граждане выдвигают кандидатов на роли **Guardian**, **Judge**, **Aiya**. Другие граждане голосуют (`For` / `Against` / `Abstain`). При наборе **5% голосов «За»** от числа всех граждан кандидатура автоматически переходит в статус **Approved**. Назначение на роль выполняет только **Айя**.

| Эндпоинт | Метод | Права |
|----------|-------|-------|
| `/candidacy/nominate` | POST | Citizen+ (active) |
| `/candidacy/:id/vote` | POST | Citizen+ (active) |
| `/candidacy/:id` | GET | Публичный |
| `/candidacy/list` | GET | Публичный |
| `/candidacy/:id/appoint` | POST | Aiya / master |

События в блокчейне: `CandidateNominated`, `CandidateVoted`, `CandidateApproved`, `CandidateAppointed`.

Граждане со статусом `suspended` или `revoked` не могут выдвигать, голосовать или быть кандидатами.

```bash
# Выдвинуть кандидата (candidate_id = UUID гражданина из citizens)
curl -s -X POST http://localhost:8080/candidacy/nominate \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"candidate_id": "citizen-uuid-bob", "target_role": "Guardian"}' | jq .

# Проголосовать «За»
curl -s -X POST http://localhost:8080/candidacy/CANDIDACY-UUID/vote \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"vote": "For"}' | jq .

# Список кандидатур (публично)
curl -s "http://localhost:8080/candidacy/list?status=Active" | jq .

# Назначить утверждённого кандидата (Айя)
curl -s -X POST http://localhost:8080/candidacy/CANDIDACY-UUID/appoint \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | jq .
```

### Инициативы (`/initiative`)

Граждане предлагают законопроекты (инициативы). Другие граждане голосуют (`For` / `Against` / `Abstain`). При наборе **5% голосов «За»** инициатива автоматически переходит в статус **Passed**.

| Эндпоинт | Метод | Права |
|----------|-------|-------|
| `/initiative/propose` | POST | Citizen+ (active) |
| `/initiative/:id/vote` | POST | Citizen+ (active) |
| `/initiative/:id` | GET | Публичный |
| `/initiative/list` | GET | Публичный |

События в блокчейне: `LawProposed`, `LawVoteStarted`, `VoteCast`, `LawVoteResult`.

```bash
curl -s -X POST http://localhost:8080/initiative/propose \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"title": "Закон о парках", "description": "Создать зелёные зоны"}' | jq .

curl -s -X POST http://localhost:8080/initiative/INITIATIVE-UUID/vote \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"vote": "For"}' | jq .

curl -s "http://localhost:8080/initiative/list?status=Proposed" | jq .
```

### Референдумы (`/referendum`)

**Айя** объявляет референдум для голосования по отмене решения. Граждане голосуют за или против.

| Эндпоинт | Метод | Права |
|----------|-------|-------|
| `/referendum/announce` | POST | Aiya / master |
| `/referendum/:id/vote` | POST | Citizen+ (active) |
| `/referendum/:id` | GET | Публичный |
| `/referendum/list` | GET | Публичный |

События в блокчейне: `ElectionAnnounced`, `VoteCast`.

```bash
curl -s -X POST http://localhost:8080/referendum/announce \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"title": "Отмена налога", "description": "Обоснование", "target_decision": "Закон о налогах"}' | jq .

curl -s -X POST http://localhost:8080/referendum/REF-UUID/vote \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"vote": "Against"}' | jq .
```

### Свод Оснований для Созидания (`/svod`)

Государство (роль **Aiya**) управляет реестром услуг; граждане просматривают каталог. Биржа принимает предложения только по кодам из Свода; `base_price` — минимальная цена в **КВАЗИ (QZ)**.

| Эндпоинт | Метод | Права |
|----------|-------|-------|
| `/svod` | GET | API key (любая роль) |
| `/svod/categories` | GET | API key |
| `/svod/service/:code` | GET | API key |
| `/svod/admin/service` | POST | Aiya / master |
| `/svod/admin/service/:code` | PUT | Aiya / master |
| `/svod/admin/service/:code` | DELETE | Aiya / master (отключить услугу) |

```bash
# Каталог услуг
curl -s http://localhost:8080/svod \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" | jq .

# Добавить услугу (Aiya)
curl -s -X POST http://localhost:8080/svod/admin/service \
  -H "Authorization: Bearer $QUAZAR_MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "code": "DESIGN_UI",
    "name": "UI Design",
    "category_code": "IT",
    "base_price": 150,
    "min_quantity": 1,
    "max_quantity": 20
  }' | jq .

# Предложение на бирже (svod_code обязателен, price >= base_price)
curl -s -X POST http://localhost:8080/exchange/offer \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"svod_code": "WEB_DEV", "price": 100, "quantity": 2}' | jq .
```

После миграции `004_service_catalog.sql` доступны seed-данные: категория `IT`, услуга `WEB_DEV` (base_price **100 QZ**).

После миграции `005_candidacies.sql` доступны таблицы `candidacies` и `candidacy_votes`.

После миграции `008_initiatives_referendums.sql` доступны таблицы `initiatives`, `initiative_votes`, `referendums`, `referendum_votes`.

### Чат (`/chat`)

Общий чат узла — per-node SQL (не блокчейн). Сообщения хранятся локально на каждом узле; для синхронизации между узлами нужен отдельный механизм (пока не реализован).

| Эндпоинт | Метод | Права |
|----------|-------|-------|
| `/chat/messages` | GET | API key (Citizen+), не node secret |
| `/chat/send` | POST | API key, только `active` граждане |

Query для списка: `limit` (1–100, по умолчанию 50), `before` (id сообщения для пагинации «старее»).

```bash
# Список последних сообщений
curl -s "http://localhost:8080/chat/messages?limit=20" \
  -H "Authorization: Bearer test_citizen_key_2026" | jq .

# Отправить сообщение
curl -s -X POST http://localhost:8080/chat/send \
  -H "Authorization: Bearer test_citizen_key_2026" \
  -H "Content-Type: application/json" \
  -d '{"content": "Привет, Квазар!"}' | jq .

# Старее указанного сообщения
curl -s "http://localhost:8080/chat/messages?before=MESSAGE-UUID&limit=20" \
  -H "Authorization: Bearer test_citizen_key_2026" | jq .
```

После миграции `007_chat.sql` доступна таблица `chat_messages`.

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
