"""Имена exchange'ей, очередей и live-адресов по `backend/messaging.md`."""

from dataclasses import dataclass
from typing import Any, Literal, Self
from uuid import UUID

EMAIL_EXCHANGE = "email"
EMAIL_TRANSACTIONAL = "send.transactional"
EMAIL_BULK = "send.bulk"

ExchangeType = Literal["topic", "direct"]


def events_exchange(service: str) -> str:
    """Факты сервиса: `<сервис>.events`, topic, ключ — тип события."""
    return f"{service}.events"


def commands_exchange(service: str) -> str:
    """Команды сервису: `<сервис>.commands`, direct, ключ — тип команды."""
    return f"{service}.commands"


def live_exchange(service: str) -> str:
    """Данные для realtime: `<сервис>.live`, topic, ключ — live-адрес."""
    return f"{service}.live"


def events_queue(consumer: str, producer: str) -> str:
    return f"{consumer}.{producer}-events"


def live_project(project_id: UUID) -> str:
    return f"project.{project_id}"


def live_investigation(project_id: UUID, investigation_id: UUID) -> str:
    return f"project.{project_id}.investigation.{investigation_id}"


def live_user(user_id: UUID) -> str:
    return f"user.{user_id}"


def live_control(kind: str) -> str:
    """`control.session.revoked`, `control.member.removed` и т.п. — у каждой реплики realtime."""
    return f"control.{kind}"


@dataclass(frozen=True)
class Destination:
    """Куда отправить сообщение из outbox."""

    exchange: str
    exchange_type: ExchangeType
    routing_key: str
    # обязательная доставка: нет очереди-получателя — ошибка; у live — False
    mandatory: bool = True

    @classmethod
    def event(cls, producer: str, type: str) -> Self:
        return cls(events_exchange(producer), "topic", type)

    @classmethod
    def command(cls, target: str, type: str) -> Self:
        return cls(commands_exchange(target), "direct", type)

    @classmethod
    def live(cls, producer: str, address: str) -> Self:
        return cls(live_exchange(producer), "topic", address, mandatory=False)

    @classmethod
    def email(cls, routing_key: str = EMAIL_TRANSACTIONAL) -> Self:
        return cls(EMAIL_EXCHANGE, "direct", routing_key)

    def to_json(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "exchange_type": self.exchange_type,
            "routing_key": self.routing_key,
            "mandatory": self.mandatory,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Self:
        return cls(
            exchange=value["exchange"],
            exchange_type=value["exchange_type"],
            routing_key=value["routing_key"],
            mandatory=value.get("mandatory", True),
        )
