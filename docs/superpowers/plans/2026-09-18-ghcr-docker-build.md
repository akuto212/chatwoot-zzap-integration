# GHCR Docker Image Build Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Проверять сборку Docker-образа в pull request и публиковать `latest` в GHCR после push в `main`.

**Architecture:** Один workflow GitHub Actions определяет два триггера и одну job. Он собирает Dockerfile на всех триггерах, но публикует образ в GHCR только при push в `main`, используя краткоживущий `GITHUB_TOKEN` и cache GitHub Actions.

**Tech Stack:** GitHub Actions, Docker Buildx, GHCR, actionlint.

---

### Task 1: GitHub Actions workflow для Docker-образа

**Files:**
- Create: `.github/workflows/docker-image.yml`
- Test: `.github/workflows/docker-image.yml` через `actionlint`

- [x] **Step 1: Убедиться, что workflow отсутствует до реализации**

Run: `test ! -e .github/workflows/docker-image.yml`

Expected: команда завершается с кодом `0`; workflow ещё не существует.

- [x] **Step 2: Создать workflow**

```yaml
name: Build and publish Docker image

on:
  pull_request:
    branches:
      - main
  push:
    branches:
      - main

permissions:
  contents: read
  packages: write

jobs:
  docker:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GHCR
        if: github.event_name == 'push'
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and publish Docker image
        uses: docker/build-push-action@v6
        with:
          context: .
          push: ${{ github.event_name == 'push' }}
          tags: ghcr.io/${{ github.repository }}:latest
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

- [x] **Step 3: Проверить синтаксис и правила GitHub Actions**

Run: `actionlint .github/workflows/docker-image.yml`

Expected: команда завершается с кодом `0` без вывода. Она подтверждает корректность YAML, событий, expressions и параметров actions.

- [x] **Step 4: Проверить изменения перед коммитом**

Run: `git diff --check && git diff -- .github/workflows/docker-image.yml`

Expected: `git diff --check` завершается с кодом `0`; diff содержит только новый workflow с `pull_request` и `push` для `main`, тегом `latest`, условной публикацией и кэшем Buildx.

- [x] **Step 5: Закоммитить workflow**

```bash
git add .github/workflows/docker-image.yml
git commit -m "ci: build and publish Docker image to GHCR"
```

Expected: Git создаёт локальный коммит, содержащий только workflow для сборки и публикации Docker-образа.
