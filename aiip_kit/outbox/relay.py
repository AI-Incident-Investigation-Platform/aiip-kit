import asyncio
import json
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
from sqlalchemy import Table, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aiip_kit.errors import DependencyUnavailableError
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.publisher import Publisher
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.tables import NOTIFY_CHANNEL

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelaySettings:
    batch_size: int = 50
    lease_seconds: int = 30
    poll_interval_seconds: float = 1.0
    max_attempts: int = 60
    max_backoff_seconds: int = 300
    # отправленные строки хранятся сутки для разбора, потом удаляются
    keep_delivered_hours: int = 24
    purge_every_seconds: int = 60


@dataclass(frozen=True)
class _Claimed:
    message_id: UUID
    destination: dict[str, Any]
    body: dict[str, Any]
    headers: dict[str, str]
    attempts: int


class OutboxRelay:
    """Отправляет outbox в брокер. Живёт в процессе сервиса, реплик сколько угодно.

    Короткая транзакция захватывает строки `FOR UPDATE SKIP LOCKED` и ставит
    lease → commit → публикация с confirm без открытой транзакции → отметка
    только при совпадении `lease_token`. Повтор после падения публикует то же
    сообщение с тем же `message_id`: получатель отсекает его inbox'ом.
    """

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        table: Table,
        publisher: Publisher,
        settings: RelaySettings | None = None,
    ) -> None:
        self._session_maker = session_maker
        self._table = table
        self._publisher = publisher
        self._settings = settings or RelaySettings()
        self._wake = asyncio.Event()

    def wake(self) -> None:
        self._wake.set()

    async def run(self, stop: asyncio.Event, listen_dsn: str | None = None) -> None:
        """Цикл до `stop`. С `listen_dsn` будится `NOTIFY outbox`, иначе только опросом."""
        listener = await self._listen(listen_dsn) if listen_dsn else None
        loop = asyncio.get_running_loop()
        next_purge = loop.time()
        try:
            while not stop.is_set():
                try:
                    sent = await self.run_once()
                    if loop.time() >= next_purge:
                        await self.purge_delivered()
                        next_purge = loop.time() + self._settings.purge_every_seconds
                except Exception:
                    log.exception("Ошибка relay outbox", extra={"event": "outbox.relay_failed"})
                    sent = 0
                if sent >= self._settings.batch_size:
                    continue
                self._wake.clear()
                waiters = {
                    asyncio.ensure_future(self._wake.wait()),
                    asyncio.ensure_future(stop.wait()),
                }
                _, pending = await asyncio.wait(
                    waiters,
                    timeout=self._settings.poll_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
        finally:
            if listener is not None:
                await listener.close()

    async def run_once(self) -> int:
        """Один проход: захватить, отправить, отметить. Возвращает число захваченных."""
        lease_token = uuid.uuid4()
        claimed = await self._claim(lease_token)
        for item in claimed:
            await self._deliver(item, lease_token)
        return len(claimed)

    async def purge_delivered(self) -> int:
        table = self._table
        cutoff = func.now() - timedelta(hours=self._settings.keep_delivered_hours)
        async with self._session_maker() as session, session.begin():
            result = await session.execute(
                delete(table).where(
                    table.c.delivered_at.is_not(None), table.c.delivered_at < cutoff
                )
            )
        return int(getattr(result, "rowcount", 0) or 0)

    async def _claim(self, lease_token: UUID) -> list[_Claimed]:
        table = self._table
        now = func.now()
        due = (
            select(table.c.message_id)
            .where(
                table.c.delivered_at.is_(None),
                table.c.parked_at.is_(None),
                table.c.available_at <= now,
                or_(table.c.lease_until.is_(None), table.c.lease_until < now),
            )
            .order_by(table.c.available_at, table.c.message_id)
            .limit(self._settings.batch_size)
            .with_for_update(skip_locked=True)
        )
        async with self._session_maker() as session, session.begin():
            rows = await session.execute(
                update(table)
                .where(table.c.message_id.in_(due.scalar_subquery()))
                .values(
                    lease_token=lease_token,
                    lease_until=now + timedelta(seconds=self._settings.lease_seconds),
                )
                .returning(
                    table.c.message_id,
                    table.c.destination,
                    table.c.body,
                    table.c.headers,
                    table.c.attempts,
                )
            )
            return [_Claimed(*row) for row in rows.all()]

    async def _deliver(self, item: _Claimed, lease_token: UUID) -> None:
        table = self._table
        owned = (table.c.message_id == item.message_id) & (table.c.lease_token == lease_token)
        try:
            message = MessageV1.model_validate(item.body)
            body = json.dumps(item.body, separators=(",", ":"), ensure_ascii=False).encode()
            await self._publisher.publish(
                Destination.from_json(item.destination), body, message, item.headers
            )
        except Exception as exc:
            attempts = item.attempts + 1
            parked = attempts >= self._settings.max_attempts or not isinstance(
                exc, DependencyUnavailableError
            )
            values: dict[str, object] = {
                "attempts": attempts,
                "lease_token": None,
                "lease_until": None,
                "last_error_code": type(exc).__name__[:64],
            }
            if parked:
                values["parked_at"] = func.now()
                log.error(
                    "Сообщение outbox отложено навсегда",
                    extra={
                        "event": "outbox.parked",
                        "outbox_message": {"id": str(item.message_id), "attempts": attempts},
                        "error": {"type": type(exc).__name__},
                    },
                )
            else:
                values["available_at"] = func.now() + timedelta(seconds=self._backoff(attempts))
            async with self._session_maker() as session, session.begin():
                await session.execute(update(table).where(owned).values(**values))
            return
        async with self._session_maker() as session, session.begin():
            await session.execute(
                update(table)
                .where(owned)
                .values(delivered_at=func.now(), lease_token=None, lease_until=None)
            )

    def _backoff(self, attempts: int) -> float:
        base = min(self._settings.max_backoff_seconds, 2**attempts)
        return base * random.uniform(0.5, 1.0)

    async def _listen(self, dsn: str) -> asyncpg.Connection:
        connection = await asyncpg.connect(dsn)
        await connection.add_listener(NOTIFY_CHANNEL, lambda *_: self._wake.set())
        return connection
