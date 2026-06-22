"""Cross-dialect column types.

The schema is PostgreSQL-native in production (UUID / JSONB), but the test suite
runs against SQLite. Rather than mutating the shared ``Base.metadata`` at test
time (fragile, leaks across tests), these ``TypeDecorator``s render the right
implementation per dialect: native ``UUID``/``JSONB`` on PostgreSQL and
``CHAR(32)``/``JSON`` on everything else. Production behaviour is unchanged.
"""

import uuid
from typing import Any

from sqlalchemy import CHAR
from sqlalchemy import JSON as SA_JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.types import TypeDecorator


class GUID(TypeDecorator):
    """Platform-independent UUID: native ``uuid`` on PostgreSQL, ``CHAR(32)``
    (hex) elsewhere. Always reads/writes ``uuid.UUID`` objects."""

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, uuid.UUID):
            value = uuid.UUID(str(value))
        if dialect.name == "postgresql":
            return value
        return value.hex

    def process_result_value(self, value: Any, dialect: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(value)


class JSONBType(TypeDecorator):
    """JSON column stored as ``JSONB`` on PostgreSQL and generic ``JSON`` (TEXT)
    elsewhere; (de)serialization is handled by the underlying impl."""

    impl = SA_JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(SA_JSON())
