# Интеграция ZZap и Chatwoot

Микросервис на Litestar для двусторонней синхронизации сообщений между ZZap и self-hosted Chatwoot.

Сервис импортирует новые сообщения из ZZap в Chatwoot, отправляет публичные исходящие сообщения операторов Chatwoot обратно в ZZap, а вложения из Chatwoot загружает в ZZap и добавляет в сообщение как ссылки на файлы. PostgreSQL является единственной stateful-зависимостью: в нем хранятся durable jobs, записи идемпотентности, маппинги, курсоры polling и состояние readiness.

## Режимы запуска

- `web`: обслуживает `/health`, `/ready` и webhook endpoint для Chatwoot.
- `worker`: опрашивает ZZap и обрабатывает durable jobs.
- `all`: запускает web и worker в одном процессе для локального Docker Compose.

В production рекомендуется запускать отдельные контейнеры `web` и `worker`, подключенные к одной базе PostgreSQL. Worker использует PostgreSQL advisory lock, поэтому только один активный worker опрашивает ZZap и обрабатывает jobs.

## Переменные окружения

Создайте `.env` на основе `.env.example` и укажите реальные секреты и идентификаторы.

Обязательные переменные:

```dotenv
APP_MODE=all
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/chatwoot_zzap
INTEGRATION_ID=11111111-1111-4111-8111-111111111111
ZZAP_BASE_URL=https://b52-api.zzap.pro
ZZAP_API_KEY=replace-me
CHATWOOT_BASE_URL=https://chatwoot.example.com
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1
CHATWOOT_API_TOKEN=replace-me
CHATWOOT_WEBHOOK_SECRET=replace-me
```

Опциональные переменные:

```dotenv
MAX_ATTACHMENT_BYTES=10485760
SUCCESSFUL_MESSAGE_RETENTION_DAYS=60
FAILED_RECORD_RETENTION_DAYS=30
WEBHOOK_DELIVERY_RETENTION_DAYS=30
```

`INTEGRATION_ID` - стабильный UUID для этой связки ZZap и Chatwoot. Первая версия поддерживает одну активную связку, настроенную через переменные окружения.

## Локальный запуск

```bash
docker compose up --build
```

Entrypoint контейнера выполняет:

```bash
uv run alembic upgrade head
```

перед запуском выбранного runtime mode. Если миграции завершаются с ошибкой, контейнер останавливается.

Локальные endpoints:

- `GET http://localhost:8000/health`
- `GET http://localhost:8000/ready`
- `POST http://localhost:8000/webhooks/chatwoot`

## Webhook Chatwoot

Настройте webhook Chatwoot для событий `message_created`:

```text
https://your-service.example.com/webhooks/chatwoot
```

Webhook должен содержать HMAC-заголовки Chatwoot:

- `X-Chatwoot-Signature`
- `X-Chatwoot-Timestamp`
- `X-Chatwoot-Delivery`

Сервис проверяет подпись через `CHATWOOT_WEBHOOK_SECRET` по строке `timestamp + "." + raw_body` и ожидает формат подписи `sha256=<hex>`. Некорректная подпись возвращает `403`.

В ZZap отправляются только публичные исходящие сообщения операторов из inbox `CHATWOOT_INBOX_ID`. Private notes, системные и bot-сообщения, входящие/импортированные сообщения, события из другого inbox и неизвестные conversation mappings игнорируются с ответом `200 OK`.

## Rate Limit ZZap

Все вызовы ZZap API проходят через один глобальный limiter: не более одного запроса каждые 3 секунды. Это включает:

- summary polling;
- загрузку сообщений отдельного thread;
- загрузку файлов;
- отправку исходящих сообщений.

Summary polling планируется каждые 3 секунды как целевой интервал, а не как гарантия. Если в очереди уже есть fetch, upload или outbound send, polling ждет своей очереди в той же FIFO-очереди. При ZZap `401` polling переходит на редкие retry, а `/ready` становится unhealthy. При `429`, rate-limit или captcha-like ответах scheduler увеличивает backoff, но readiness остается healthy.

## Известные ограничения

- Отслеживается только первая страница ZZap threads: `page=1&page_size=100`.
- Сообщения ZZap не имеют стабильных message IDs, поэтому дедупликация использует синтетический fingerprint.
- Полностью одинаковые сообщения от одного отправителя в одну и ту же секунду могут быть обработаны как дубликаты.
- Старая прочитанная история ZZap не импортируется.
- Состояние read/unread не синхронизируется.
- Редактирование и удаление сообщений в Chatwoot не синхронизируются.
- Редактирование и удаление сообщений в ZZap не синхронизируются.
- Вложения из ZZap в Chatwoot не конвертируются в нативные вложения Chatwoot; ссылки остаются текстом.
- Chatwoot reconciliation polling не реализован в первой версии.

## Тесты

```bash
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run ruff check .
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run mypy app
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q
```
