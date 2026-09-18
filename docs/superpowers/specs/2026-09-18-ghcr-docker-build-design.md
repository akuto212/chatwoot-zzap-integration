# Сборка Docker-образа в GitHub Actions

## Цель

Автоматически проверять сборку Docker-образа сервиса для pull request в `master` и публиковать актуальный образ в GitHub Container Registry после каждого push в `master`.

## Workflow

Добавляется один workflow `.github/workflows/docker-image.yml`.

Он запускается при:

- `pull_request` в `master` — образ только собирается, публикации нет;
- `push` в `master` — образ собирается и публикуется в GHCR.

Теги Git не являются триггером workflow и не создают отдельные Docker-теги.

## Образ и доступ

Образ публикуется как `ghcr.io/<owner>/chatwoot-zzap-integration:latest`, где `<owner>` и имя репозитория берутся из `github.repository`. Workflow использует встроенный `GITHUB_TOKEN`; отдельный секрет не требуется.

Workflow получает минимальные необходимые права:

- `contents: read` для checkout репозитория;
- `packages: write` для публикации в GHCR.

## Реализация

Workflow настроит Docker Buildx, выполнит вход в `ghcr.io` только для push в `master`, соберёт Dockerfile из корня репозитория и задействует cache GitHub Actions для слоёв Docker. При pull request параметр публикации отключён, поэтому workflow проверяет, что Dockerfile и build context остаются рабочими, без изменения registry.

Сбои checkout, аутентификации, Docker build или публикации завершают job с ошибкой и видны в GitHub Actions.

## Проверка

Конфигурация workflow будет проверена actionlint. Синтаксис и параметры Docker workflow будут проверены локально; фактическая публикация происходит только в GitHub Actions после merge/push в `master`.
