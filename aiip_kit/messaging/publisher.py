import asyncio
from collections.abc import Mapping
from typing import Protocol

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection
from aio_pika.exceptions import AMQPError, DeliveryError, PublishError

from aiip_kit.errors import DependencyUnavailableError
from aiip_kit.messaging.amqp import amqp_message
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination


class Publisher(Protocol):
    async def publish(
        self,
        destination: Destination,
        body: bytes,
        message: MessageV1,
        headers: Mapping[str, str],
    ) -> None:
        """Публикует и ждёт подтверждения брокера.

        Raises:
            DependencyUnavailableError: брокер недоступен, не подтвердил или вернул сообщение.
        """
        ...


class AmqpPublisher:
    """Публикация с publisher confirm. Соединение — при первой публикации, дальше само.

    Exchange объявляется идемпотентно тем же типом и durable: его владелец и
    получатели объявляют его так же.
    """

    def __init__(self, url: str, *, connect_timeout: float = 5, publish_timeout: float = 5) -> None:
        self._url = url
        self._connect_timeout = connect_timeout
        self._publish_timeout = publish_timeout
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchanges: dict[str, AbstractExchange] = {}
        self._connecting = asyncio.Lock()

    async def publish(
        self,
        destination: Destination,
        body: bytes,
        message: MessageV1,
        headers: Mapping[str, str],
    ) -> None:
        try:
            exchange = await self._exchange(destination)
            await exchange.publish(
                amqp_message(body, message, headers),
                routing_key=destination.routing_key,
                mandatory=destination.mandatory,
                timeout=self._publish_timeout,
            )
        except (TimeoutError, DeliveryError, PublishError, AMQPError, OSError) as exc:
            raise DependencyUnavailableError("rabbitmq") from exc

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()

    async def _exchange(self, destination: Destination) -> AbstractExchange:
        if (exchange := self._exchanges.get(destination.exchange)) is not None:
            return exchange
        async with self._connecting:
            if self._channel is None:
                connection = await aio_pika.connect_robust(self._url, timeout=self._connect_timeout)
                try:
                    self._channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                except BaseException:
                    await connection.close()
                    raise
                self._connection = connection
            if destination.exchange not in self._exchanges:
                self._exchanges[destination.exchange] = await self._channel.declare_exchange(
                    destination.exchange,
                    aio_pika.ExchangeType(destination.exchange_type),
                    durable=True,
                )
            return self._exchanges[destination.exchange]


class FakePublisher:
    """Публикатор для тестов: запоминает отправленное, может падать по заказу."""

    def __init__(self) -> None:
        self.published: list[tuple[Destination, MessageV1, dict[str, str]]] = []
        self.fail_next = 0

    async def publish(
        self,
        destination: Destination,
        body: bytes,
        message: MessageV1,
        headers: Mapping[str, str],
    ) -> None:
        if self.fail_next > 0:
            self.fail_next -= 1
            raise DependencyUnavailableError("rabbitmq")
        self.published.append((destination, message, dict(headers)))
