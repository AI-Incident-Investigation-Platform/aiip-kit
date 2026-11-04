"""Outbox, relay и inbox на настоящем Postgres: SKIP LOCKED, lease, ON CONFLICT."""

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
from sqlalchemy import MetaData, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from aiip_kit.errors import InboxConflictError
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.publisher import FakePublisher
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.inbox import InboxGuard
from aiip_kit.outbox.relay import OutboxRelay, RelaySettings
from aiip_kit.outbox.tables import inbox_table, outbox_table
from aiip_kit.outbox.writer import OutboxWriter

metadata = MetaData()
OUTBOX = outbox_table(metadata)
INBOX = inbox_table(metadata)
DESTINATION = Destination.event("incidents", "incident.created")


@pytest.fixture(scope="module")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:18-alpine", driver="asyncpg") as container:
        yield container


@pytest.fixture
async def sessions(postgres: PostgresContainer) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres.get_connection_url())
    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


def event(n: int = 1) -> MessageV1:
    return MessageV1.event("incident.created", "incidents", {"n": n})


async def write(sessions: async_sessionmaker[AsyncSession], *messages: MessageV1) -> None:
    async with sessions() as session, session.begin():
        for message in messages:
            await OutboxWriter(OUTBOX).add(session, message, DESTINATION)


async def test_rolled_back_transaction_leaves_nothing(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as session:
        await session.begin()
        await OutboxWriter(OUTBOX).add(session, event(), DESTINATION)
        await session.rollback()

    publisher = FakePublisher()
    assert await OutboxRelay(sessions, OUTBOX, publisher).run_once() == 0
    assert publisher.published == []


async def test_relay_publishes_once_and_marks_delivered(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    message = event()
    await write(sessions, message)
    publisher = FakePublisher()
    relay = OutboxRelay(sessions, OUTBOX, publisher)

    assert await relay.run_once() == 1
    assert await relay.run_once() == 0
    assert [m.message_id for _, m, _ in publisher.published] == [message.message_id]


async def test_parallel_relays_do_not_share_rows(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await write(sessions, *(event(n) for n in range(40)))
    publisher = FakePublisher()
    settings = RelaySettings(batch_size=10)
    relays = [OutboxRelay(sessions, OUTBOX, publisher, settings) for _ in range(4)]

    while sum(await asyncio.gather(*(relay.run_once() for relay in relays))):
        pass

    ids = [m.message_id for _, m, _ in publisher.published]
    assert len(ids) == len(set(ids)) == 40


async def test_broker_failure_is_retried_later(sessions: async_sessionmaker[AsyncSession]) -> None:
    await write(sessions, event())
    publisher = FakePublisher()
    publisher.fail_next = 1
    relay = OutboxRelay(sessions, OUTBOX, publisher)

    await relay.run_once()
    async with sessions() as session:
        row = (await session.execute(select(OUTBOX))).one()
    assert row.attempts == 1 and row.delivered_at is None and row.parked_at is None

    async with sessions() as session, session.begin():
        await session.execute(update(OUTBOX).values(available_at=func.now()))
    await relay.run_once()
    assert len(publisher.published) == 1


async def test_inbox_accepts_once_and_flags_conflict(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    guard = InboxGuard(INBOX)
    message = event()

    async with sessions() as session, session.begin():
        assert await guard.accept(session, "notifications.incident_created", message)
    async with sessions() as session, session.begin():
        assert not await guard.accept(session, "notifications.incident_created", message)
    async with sessions() as session, session.begin():
        assert await guard.accept(session, "realtime.incident_created", message)

    changed = message.model_copy(update={"data": {"n": 2}})
    async with sessions() as session, session.begin():
        with pytest.raises(InboxConflictError):
            await guard.accept(session, "notifications.incident_created", changed)
