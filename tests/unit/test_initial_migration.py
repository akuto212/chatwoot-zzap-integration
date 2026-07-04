from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


def test_initial_migration_enum_types_are_not_auto_created_by_tables() -> None:
    migration = _load_initial_migration()

    for enum_name in ("message_direction", "message_status", "job_type", "job_status"):
        enum_type = getattr(migration, enum_name)

        assert enum_type.create_type is False


def _load_initial_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "0001_initial_schema.py"
    )
    spec = importlib.util.spec_from_file_location("initial_schema_migration", migration_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load initial migration spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
