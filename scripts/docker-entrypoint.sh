#!/bin/sh
set -eu

uv run alembic upgrade head

if [ "${APP_MODE:-web}" = "web" ]; then
  exec uv run uvicorn app.asgi:app --host 0.0.0.0 --port 8000
fi

if [ "${APP_MODE:-web}" = "worker" ]; then
  exec uv run python -m app.cli
fi

if [ "${APP_MODE:-web}" = "all" ]; then
  exec uv run python -m app.cli
fi

echo "Unsupported APP_MODE=${APP_MODE:-web}" >&2
exit 1
