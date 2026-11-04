from sqlalchemy import Table, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aiip_kit.errors import InboxConflictError
from aiip_kit.messaging.message import MessageV1


class InboxGuard:
    """Отсекает повторную доставку в транзакции обработчика.

    Вызывается первым в той же транзакции, что и изменение: повтор после
    commit найдёт запись и ничего не сделает, повтор после отката — не найдёт и
    обработает заново.
    """

    def __init__(self, table: Table) -> None:
        self._table = table

    async def accept(self, session: AsyncSession, handler: str, message: MessageV1) -> bool:
        """True — сообщение новое, обрабатывай; False — уже обработано, просто ack.

        Raises:
            InboxConflictError: тот же `message_id` уже пришёл с другим телом.
        """
        payload_hash = message.payload_hash()
        table = self._table
        inserted = await session.execute(
            insert(table)
            .values(handler=handler, message_id=message.message_id, payload_hash=payload_hash)
            .on_conflict_do_nothing()
            .returning(table.c.message_id)
        )
        if inserted.first() is not None:
            return True
        stored = await session.scalar(
            select(table.c.payload_hash).where(
                table.c.handler == handler, table.c.message_id == message.message_id
            )
        )
        if stored != payload_hash:
            raise InboxConflictError(f"{handler}: {message.message_id} with different payload")
        return False
