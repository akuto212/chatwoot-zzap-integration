# Наблюдаемость ZZap

Наблюдение не меняет polling, лимит 3 секунды, backoff, retry, авторизацию,
порядок запросов, пагинацию, дедупликацию или sync_jobs. Новых запросов ZZap нет.
Новых сервисов, зависимостей и миграций БД нет.

## Настройки

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `ZZAP_POLL_DEGRADED_SECONDS` | `120` | Порог без успешного получения списка тредов, > 0 |
| `METRICS_TOKEN` | пусто | Пусто: `/metrics` выключен (404); задано: обязательный Bearer token |
| `METRICS_CACHE_SECONDS` | `5` | TTL одного небольшого SELECT service_state на web-процесс |
| `METRICS_DB_TIMEOUT_SECONDS` | `5` | Максимальное время чтения БД для scrape |

Worker сохраняет свой порог в состоянии; exporter использует его. Рекомендуется
одинаковая конфигурация web/worker и обязательны общие DATABASE_URL/INTEGRATION_ID.
METRICS_TOKEN — отдельный случайный секрет, не ZZap API key. Доступ дополнительно
ограничить приватной сетью или reverse proxy; через публичную сеть использовать TLS.
Не публиковать `/metrics` через общедоступный webhook ingress.

## Состояние и события

`service_state.zzap_poll_health` хранит initialized_at, last_attempt_at,
last_attempt_status (in_progress/success/failure), last_success_at,
consecutive_failures, failure_since, incident_started_at, last_error_category, last_http_status,
degraded/degraded_at, last_recovery_at и last_incident (начало, окончание,
число ошибок и длительность последнего завершённого инцидента).

Только корректный результат list_threads сбрасывает сбой. POST, upload и GET
отдельного треда не влияют на polling health. Ошибка последующей обработки
тредов в БД также не считается ошибкой получения списка. Начало инцидента фиксируется в incident_started_at и не меняется, если после
зависшего polling начинается серия HTTP-ошибок; failure_since хранит начало
самой серии ошибок. без истории успешных чтений отсчёт порога идёт от initialized_at.
После перезапуска состояние продолжается, а следующий сбой после восстановления
создаёт новый инцидент. Текущая длительность рассчитывается из timestamps.

Worker пишет два небольших snapshot на summary poll: начало и результат. Это
сохраняет незавершённую попытку при рестарте. Независимый timer проверяет деградацию
без запросов БД и API, в том числе во время backoff или медленного HTTP-запроса;
он пишет только переход и повторяет неудавшуюся запись не чаще раз в 5 секунд.
При исправной БД/работающем event loop деградация определяется на пороге
(с обычной задержкой планирования asyncio). Недоступная БД не даёт гарантий
сохранения; ошибка exporter даёт 503, состояние не маскируется вечным кэшем.
Ошибки записи наблюдения не заменяют результат ZZap и его backoff.

JSON logs в stderr:

- `zzap_api_request_failed`: UTC timestamp, method, шаблон endpoint, page/page_size,
  HTTP status, duration_seconds, category, source и фильтрованные headers/body metadata.
- `zzap_poll_degraded`: один WARNING при переходе, начало инцидента и число ошибок.
- `zzap_poll_recovered`: INFO при восстановлении, начало/окончание, длительность и ошибки.
- `zzap_poll_health_persistence_failed`: безопасный сигнал проблем сохранения наблюдения.

Тело не копируется: только формат, размер, SHA256, bounded numeric code. Произвольные
error/message fields, user_key, текст сообщений, контактные данные, токены,
Authorization и Cookie не логируются. Request/trace/CDN IDs проходят строгую проверку
формата; неизвестные headers отбрасываются. IP назначения извлекается только из
имеющегося HTTP stream, если доступен. HTTPX/httpcore INFO/DEBUG подавлены, поскольку
они раскрывают полный thread URL. Не включать их DEBUG в production.

`source=api_envelope_suspected` означает узнаваемую JSON-структуру, а
`cdn_waf_suspected` — HTML + признак CDN; это гипотезы, не доказательство.
Обычный JSON/HTML без признаков остаётся `unknown`. `429` — category rate_limit;
`401` authentication, `403` forbidden, `5xx` http_server. Сетевые категории:
timeout, dns, tls, connection_reset, tcp, connection/network_unknown. Если библиотека
не сохраняет cause, источник сетевой ошибки остаётся неизвестным.

## Prometheus

Собирать с web/all процесса; отдельный worker отдаёт состояние через PostgreSQL.
Вся метрика относится к одной INTEGRATION_ID, динамических labels нет. Настроить
job/instance labels в Prometheus. Timestamp 0 означает отсутствие истории.

```yaml
scrape_configs:
  - job_name: chatwoot-zzap
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /run/secrets/zzap_metrics_token
    static_configs:
      - targets: ['integration-web:8000']
rule_files:
  - /etc/prometheus/zzap-alerts.yml
```

Секрет credentials_file должен совпадать с METRICS_TOKEN web. Подключить
[правила](prometheus-alerts.yml) к существующему Prometheus/Alertmanager.
Проверить конфигурацию `promtool check config` и `promtool check rules` перед
загрузкой. Встроенной отправки Telegram нет.

Gauges: `zzap_last_success_timestamp_seconds`, `zzap_last_attempt_timestamp_seconds`,
`zzap_poll_initialized_timestamp_seconds`, `zzap_poll_consecutive_failures`,
`zzap_poll_degraded`, `zzap_poll_failure_duration_seconds`, `zzap_poll_last_http_status`,
`zzap_worker_last_heartbeat_timestamp_seconds`. Кэшируется snapshot, но время/деградация
пересчитываются на каждом scrape. Счётчики всех запросов не добавлены: корректные
агрегаты между web/worker и рестартами потребовали бы лишних записей/инфраструктуры.

Первый alert возникает после 120 секунд + scrape/evaluation + `for: 15s`;
порог worker события остаётся 120 секунд. HTTP-errors alert отделён от network errors.
Heartbeat alert имеет `for: 30s`; metrics up=0 — `for: 1m`. Полное отсутствие
worker history закрывается отдельным startup alert `for: 2m`.

## Grafana

Добавить существующий Prometheus как datasource. Создать Stat для
`zzap_poll_degraded` (0 зелёный/1 красный), Stat для HTTP status и error count;
Time series для `zzap_poll_failure_duration_seconds`,
`time() - zzap_last_success_timestamp_seconds` (фильтр last_success > 0),
`time() - zzap_worker_last_heartbeat_timestamp_seconds`.
Timestamps показывать как date/time, значения 0 как «ещё не было».
Logs брать из существующего сборщика stderr, фильтр event=zzap_api_request_failed
и zzap_poll_degraded/zzap_poll_recovered; не переносить диагностические IDs в labels.

## Повторный 403 и аудит лимита

Сопоставить UTC last_success/attempt, failure_since, degraded и recovered.
Сначала проверить heartbeat и PostgreSQL: старый heartbeat указывает на остановку
или зависание worker; свежий heartbeat + forbidden на недоступность входящего API.
Сохранить safe logs за интервал, CF-Ray/request ID, Retry-After/rate-limit headers,
формат/хеш тела. HTML с CDN признаками обсудить с ZZap как возможный WAF/CDN;
JSON envelope как возможную API-ошибку. Без признаков причина неизвестна.
Передать ZZap время UTC, duration, status, endpoint template и safe diagnostic IDs,
публичный egress IP из инфраструктуры; не передавать ключи, сообщения и user_key.
Часовой 403 сам по себе не доказывает rate limit. Не менять IP/UA/auth/polling.

Аудит текущего кода: один ZZapRateLimiter(3.0) создаётся run_worker_loop;
GET summary и thread проходят process_next_zzap_action, POST/upload —
RateLimitedZZapClient с тем же экземпляром; finally учитывает ошибки и exceptions.
Пагинация сообщений использует result_info.total_count (с прежним fallback);
каждый list_messages_page ждёт тот же limiter и отмечает завершение в finally,
включая отказ на промежуточной странице.
Все действия последовательны. Web не вызывает ZZap. В all один worker.
Session advisory lock на отдельном соединении исключает второй worker той же БД.

Границы гарантии: отдельные БД/потребители одного ключа или IP не координируются.
При потере выделенного lock-соединения (например, перезапуск PostgreSQL) advisory
lock исчезает; текущий цикл не проверяет его заново, а рабочие pool соединения
могут восстановиться. Другой worker может взять lock, образовав два независимых
limiter. Быстрый рестарт также теряет in-memory finish timestamp и может сделать
первый запрос раньше 3 секунд после предыдущего worker. Это конкретные условные
риски существующей реализации, а не установленная причина инцидента 2 октября;
исправления lock lifetime/persistent limiter требуют отдельного согласования.

Для фактического аудита проверить число процессов/контейнеров и общность БД,
внешних клиентов ключа/IP и историю рестартов БД/worker. Сопоставить уже имеющиеся
серверные/access traces ZZap и HTTP completion timestamps: минимум 3 секунды от
окончания любого запроса до старта следующего. Rate-limit unit tests покрывают
общий limiter для GET/POST/upload и ошибки. Не включать full URL/request logging
ради аудита; polling gauges не являются счётчиком всех ZZap запросов.

## Health и безопасное обновление

`/health` — liveness, не обращается к БД/API. `/ready` сохраняет проверки PostgreSQL
и zzap_auth_failed/chatwoot_auth_failed. Внешний 401 способен делать ready=503 по
прежней семантике, но не должен инициировать restart через liveness. В production
example Docker healthcheck использует /health; отдельный readiness probe — /ready.
Worker heartbeat/polling — Prometheus, а не контейнерные restart probes.

Обновлять штатным управляемым процессом после review и backup согласно вашей
практике; новых миграций нет. Проверить единичный worker и прекратить старый перед
стартом нового; выдержать не менее 3 секунд после последнего ZZap запроса старого.
Прописать настройки, токен/сетевое ограничение и scrape/rules; проверить 401 без
токена, 200 с токеном, gauges/heartbeat. Проверить controlled fixture 403 в staging,
не блокировать production API искусственно. Секреты .env, БД и production процессы
при разработке не менялись. Rollback — предыдущий image/config; дополнительная
строка service_state безопасно игнорируется старым кодом.
