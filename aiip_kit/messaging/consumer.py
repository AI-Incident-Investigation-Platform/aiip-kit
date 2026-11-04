import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage, AbstractQueue
from opentelemetry import context, propagate, trace
from pydantic import ValidationError

from aiip_kit.errors import InboxConflictError, TransientError
from aiip_kit.messaging.amqp import ATTEMPT_HEADER, from_amqp
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import ExchangeType

log = logging.getLogger(__name__)

# повторы по нарастающей: первые три — через 5 с, до десятой — через 30 с, дальше — через 5 минут
RETRY_DELAYS_SECONDS = (5, 30, 300)
DEAD_TTL_DAYS = 14

Handler = Callable[[MessageV1, int], Awaitable[None]]


@dataclass(frozen=True)
class Binding:
    exchange: str
    exchange_type: ExchangeType
    routing_key: str


@dataclass(frozen=True)
class QueueSpec:
    """Очередь потребителя: имя по `backend/messaging.md`, привязки, пределы."""

    name: str
    bindings: Sequence[Binding]
    max_length: int = 10_000
    max_bytes: int = 32 * 1024 * 1024
    prefetch: int = 8
    max_attempts: int = 60
    transient: tuple[type[BaseException], ...] = field(default=(TransientError,))


def retry_delay(attempt: int) -> int:
    """Задержка перед попыткой `attempt + 1`."""
    if attempt <= 3:
        return RETRY_DELAYS_SECONDS[0]
    if attempt <= 10:
        return RETRY_DELAYS_SECONDS[1]
    return RETRY_DELAYS_SECONDS[2]


def retry_queue(queue: str, delay_seconds: int) -> str:
    return f"{queue}.retry-{delay_seconds}s"


class Consumer:
    """Читает очередь `MessageV1` и решает судьбу сообщения по исходу обработчика.

    Обработчик сам открывает транзакцию: inbox + изменение + outbox, commit.
    Успех — ack. Временный отказ (`spec.transient`) — копия в retry-очередь с
    подтверждением брокера, потом ack оригинала; после `max_attempts` — dead.
    Битое тело, конфликт inbox и любая другая ошибка — сразу dead. Retry и
    dead — quorum-очереди рядом с основной, идут через exchange по умолчанию.
    """

    def __init__(self, channel: AbstractChannel, spec: QueueSpec, handler: Handler) -> None:
        self._channel = channel
        self._spec = spec
        self._handler = handler
        self._tracer = trace.get_tracer(__name__)

    async def start(self) -> AbstractQueue:
        """Объявляет очереди и привязки и начинает приём; канал — с publisher confirms."""
        await self._channel.set_qos(prefetch_count=self._spec.prefetch)
        queue = await declare_topology(self._channel, self._spec)
        await queue.consume(self.handle)
        log.info(
            "Начат приём сообщений",
            extra={"event": "consumer.started", "queue": self._spec.name},
        )
        return queue

    async def handle(self, incoming: AbstractIncomingMessage) -> None:
        headers = incoming.headers or {}
        attempt = int(str(headers.get(ATTEMPT_HEADER, 1)))
        token = context.attach(propagate.extract({k: str(v) for k, v in headers.items()}))
        try:
            with self._tracer.start_as_current_span(f"consume {self._spec.name}"):
                await self._process(incoming, attempt)
        finally:
            context.detach(token)

    async def _process(self, incoming: AbstractIncomingMessage, attempt: int) -> None:
        try:
            message = from_amqp(incoming.body)
        except ValidationError:
            await self._dead(incoming, attempt, "invalid_body")
            return
        try:
            await self._handler(message, attempt)
        except self._spec.transient as exc:
            if attempt >= self._spec.max_attempts:
                await self._dead(incoming, attempt, "attempts_exhausted", exc)
                return
            await self._retry(incoming, attempt)
            await incoming.ack()
            log.warning(
                "Сообщение отложено на повтор",
                extra=self._fields("consumer.retried", incoming, attempt, exc),
            )
        except InboxConflictError as exc:
            await self._dead(incoming, attempt, "inbox_conflict", exc)
        except Exception as exc:
            await self._dead(incoming, attempt, "handler_failed", exc)
        else:
            await incoming.ack()

    async def _retry(self, incoming: AbstractIncomingMessage, attempt: int) -> None:
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                incoming.body,
                message_id=incoming.message_id,
                type=incoming.type,
                content_type=incoming.content_type,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers={**(incoming.headers or {}), ATTEMPT_HEADER: attempt + 1},
            ),
            routing_key=retry_queue(self._spec.name, retry_delay(attempt)),
        )

    async def _dead(
        self,
        incoming: AbstractIncomingMessage,
        attempt: int,
        reason: str,
        exc: BaseException | None = None,
    ) -> None:
        await incoming.reject(requeue=False)
        log.error(
            "Сообщение не обработано и отправлено в dead",
            exc_info=exc if exc is not None and not isinstance(exc, TransientError) else None,
            extra={**self._fields("consumer.dead", incoming, attempt, exc), "reason": reason},
        )

    def _fields(
        self,
        event: str,
        incoming: AbstractIncomingMessage,
        attempt: int,
        exc: BaseException | None,
    ) -> dict[str, object]:
        fields: dict[str, object] = {
            "event": event,
            "queue": self._spec.name,
            "amqp_message": {"id": incoming.message_id, "type": incoming.type, "attempt": attempt},
        }
        if exc is not None:
            fields["error"] = {"type": type(exc).__name__}
        return fields


async def declare_topology(channel: AbstractChannel, spec: QueueSpec) -> AbstractQueue:
    """Dead, retry по каждой задержке и основная очередь с привязками; всё идемпотентно."""
    await channel.declare_queue(
        f"{spec.name}.dead",
        durable=True,
        arguments={"x-queue-type": "quorum", "x-message-ttl": DEAD_TTL_DAYS * 86_400_000},
    )
    for delay in RETRY_DELAYS_SECONDS:
        await channel.declare_queue(
            retry_queue(spec.name, delay),
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": delay * 1000,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": spec.name,
            },
        )
    queue = await channel.declare_queue(
        spec.name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-max-length": spec.max_length,
            "x-max-length-bytes": spec.max_bytes,
            "x-overflow": "reject-publish",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{spec.name}.dead",
            "x-dead-letter-strategy": "at-least-once",
        },
    )
    for binding in spec.bindings:
        exchange = await channel.declare_exchange(
            binding.exchange, aio_pika.ExchangeType(binding.exchange_type), durable=True
        )
        await queue.bind(exchange, routing_key=binding.routing_key)
    return queue
