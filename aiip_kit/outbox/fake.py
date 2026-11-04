from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination


class FakeOutboxWriter:
    """Запоминает сообщения вместо записи в БД: для unit-тестов сервисной логики."""

    def __init__(self) -> None:
        self.messages: list[tuple[MessageV1, Destination]] = []

    async def add(
        self,
        session: AsyncSession | None,
        message: MessageV1,
        destination: Destination,
        *,
        available_at: datetime | None = None,
    ) -> None:
        self.messages.append((message, destination))

    def types(self) -> list[str]:
        return [message.type for message, _ in self.messages]
