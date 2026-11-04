"""Таблицы outbox и inbox в метаданных сервиса: миграции генерирует и применяет сам сервис."""

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

NOTIFY_CHANNEL = "outbox"


def outbox_table(metadata: MetaData, name: str = "outbox") -> Table:
    return Table(
        name,
        metadata,
        Column("message_id", Uuid, primary_key=True),
        Column("destination", JSONB, nullable=False),
        Column("body", JSONB, nullable=False),
        Column("headers", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("available_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("lease_token", Uuid),
        Column("lease_until", DateTime(timezone=True)),
        Column("attempts", Integer, nullable=False, server_default=text("0")),
        Column("delivered_at", DateTime(timezone=True)),
        Column("parked_at", DateTime(timezone=True)),
        Column("last_error_code", String(64)),
        # частичный индекс по ждущим: relay не сканирует отправленную историю
        Index(
            f"ix_{name}_pending",
            "available_at",
            "message_id",
            postgresql_where=text("delivered_at IS NULL AND parked_at IS NULL"),
        ),
    )


def inbox_table(metadata: MetaData, name: str = "inbox") -> Table:
    return Table(
        name,
        metadata,
        Column("handler", String(128), nullable=False),
        Column("message_id", Uuid, nullable=False),
        Column("payload_hash", String(64), nullable=False),
        Column("received_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        PrimaryKeyConstraint("handler", "message_id", name=f"pk_{name}"),
        Index(f"ix_{name}_received_at", "received_at"),
    )
