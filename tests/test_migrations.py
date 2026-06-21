from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from src.database import Base


def test_alembic_config_loads():
    config = Config("alembic.ini")
    script = ScriptDirectory.from_config(config)
    heads = script.get_heads()
    assert isinstance(heads, list)
    assert len(heads) >= 1, "At least one migration head must exist"


def test_initial_migration_file_exists():
    versions_dir = Path("alembic/versions")
    migration_files = list(versions_dir.glob("*.py"))
    migration_files = [f for f in migration_files if f.name != "__init__.py"]
    assert len(migration_files) >= 1, "At least one migration file must exist in alembic/versions/"


def test_models_registered_for_migration():
    table_names = set(Base.metadata.tables.keys())
    assert "users" in table_names
    assert "raw_payloads" in table_names
    assert "sync_logs" in table_names
    assert "normalized_events" in table_names
    assert "event_versions" in table_names


def test_migration_covers_all_tables():
    versions_dir = Path("alembic/versions")
    migration_files = list(versions_dir.glob("*.py"))
    migration_files = [f for f in migration_files if f.name != "__init__.py"]
    assert len(migration_files) >= 1

    all_content = ""
    for f in migration_files:
        all_content += f.read_text()

    for table in Base.metadata.tables:
        assert table in all_content, f"Table '{table}' not found in migration files"
