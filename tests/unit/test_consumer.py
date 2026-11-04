from typing import Any, cast

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage

from aiip_kit.errors import InboxConflictError, TransientError
from aiip_kit.messaging.amqp import ATTEMPT_HEADER, to_amqp
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec, retry_delay
from aiip_kit.messaging.message import MessageV1

SPEC = QueueSpec(
    name="notifications.incidents-events",
    bindings=[Binding("incidents.events", "topic", "incident.created")],
    max_attempts=3,
)


class FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[aio_pika.Message, str]] = []

    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        self.published.append((message, routing_key))


class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = FakeExchange()


class FakeIncoming:
    def __init__(self, body: bytes, attempt: int = 1) -> None:
        self.body = body
        self.headers: dict[str, Any] = {ATTEMPT_HEADER: attempt}
        self.message_id = "m-1"
        self.type = "incident.created"
        self.content_type = "application/json"
        self.outcome: str | None = None

    async def ack(self) -> None:
        self.outcome = "ack"

    async def reject(self, requeue: bool = False) -> None:
        self.outcome = "dead"


def consumer(handler: Any) -> tuple[Consumer, FakeChannel]:
    channel = FakeChannel()
    return Consumer(cast(AbstractChannel, channel), SPEC, handler), channel


def body() -> bytes:
    return to_amqp(MessageV1.event("incident.created", "incidents", {"id": 1})).body


async def handle(target: Consumer, incoming: FakeIncoming) -> None:
    await target.handle(cast(AbstractIncomingMessage, incoming))


async def test_success_is_acked() -> None:
    seen: list[tuple[str, int]] = []

    async def handler(message: MessageV1, attempt: int) -> None:
        seen.append((message.type, attempt))

    target, _ = consumer(handler)
    incoming = FakeIncoming(body())
    await handle(target, incoming)

    assert incoming.outcome == "ack"
    assert seen == [("incident.created", 1)]


async def test_transient_failure_goes_to_retry_with_next_attempt() -> None:
    async def handler(message: MessageV1, attempt: int) -> None:
        raise TransientError("db down")

    target, channel = consumer(handler)
    incoming = FakeIncoming(body(), attempt=2)
    await handle(target, incoming)

    assert incoming.outcome == "ack"
    retried, routing_key = channel.default_exchange.published[0]
    assert routing_key == "notifications.incidents-events.retry-5s"
    assert retried.headers[ATTEMPT_HEADER] == 3


async def test_exhausted_attempts_go_to_dead() -> None:
    async def handler(message: MessageV1, attempt: int) -> None:
        raise TransientError("db down")

    target, channel = consumer(handler)
    incoming = FakeIncoming(body(), attempt=3)
    await handle(target, incoming)

    assert incoming.outcome == "dead"
    assert channel.default_exchange.published == []


@pytest.mark.parametrize("error", [InboxConflictError("x"), ValueError("bug")])
async def test_permanent_failure_goes_to_dead(error: Exception) -> None:
    async def handler(message: MessageV1, attempt: int) -> None:
        raise error

    target, _ = consumer(handler)
    incoming = FakeIncoming(body())
    await handle(target, incoming)

    assert incoming.outcome == "dead"


async def test_invalid_body_goes_to_dead_without_handler() -> None:
    async def handler(message: MessageV1, attempt: int) -> None:
        raise AssertionError("must not be called")

    target, _ = consumer(handler)
    incoming = FakeIncoming(b'{"not": "a message"}')
    await handle(target, incoming)

    assert incoming.outcome == "dead"


def test_retry_delay_grows() -> None:
    assert [retry_delay(a) for a in (1, 3, 4, 10, 11, 59)] == [5, 5, 30, 30, 300, 300]
