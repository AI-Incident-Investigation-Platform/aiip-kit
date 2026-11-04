from datetime import datetime

from opentelemetry import propagate
from sqlalchemy import Table, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.tables import NOTIFY_CHANNEL


class OutboxWriter:
    """Кладёт сообщение в outbox в транзакции вызывающего; commit делает сервис.

    Событие уходит в брокер, только если транзакция с изменением закоммитилась.
    `NOTIFY` доставляется тоже при commit и будит relay без ожидания опроса.
    """

    def __init__(self, table: Table) -> None:
        self._table = table

    async def add(
        self,
        session: AsyncSession,
        message: MessageV1,
        destination: Destination,
        *,
        available_at: datetime | None = None,
    ) -> None:
        headers: dict[str, str] = {}
        propagate.inject(headers)
        values: dict[str, object] = {
            "message_id": message.message_id,
            "destination": destination.to_json(),
            "body": message.model_dump(mode="json"),
            "headers": headers,
        }
        if available_at is not None:
            values["available_at"] = available_at
        await session.execute(insert(self._table).values(**values))
        await session.execute(select(text("pg_notify(:channel, '')")), {"channel": NOTIFY_CHANNEL})
