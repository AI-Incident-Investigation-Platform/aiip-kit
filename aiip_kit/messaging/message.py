import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Actor(BaseModel):
    """Кто вызвал изменение: пользователь по id или сервис по имени."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["user", "service"]
    id: str


class MessageV1(BaseModel):
    """Конверт события или команды в RabbitMQ; поля — `backend/messaging.md`.

    Порядок изменений одного объекта — `aggregate_version`, а не `occurred_at`:
    часы разных подов расходятся. Токены и секреты в `data` не кладутся.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    message_id: UUID = Field(default_factory=uuid.uuid7)
    kind: Literal["event", "command"]
    type: str
    producer: str
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    aggregate_type: str | None = None
    aggregate_id: UUID | None = None
    aggregate_version: int | None = None
    project_id: UUID | None = None
    lifecycle_version: int | None = None
    actor: Actor | None = None
    correlation_id: UUID | None = None
    causation_id: UUID | None = None
    expires_at: AwareDatetime | None = None
    data: dict[str, Any]

    @classmethod
    def event(cls, type: str, producer: str, data: dict[str, Any], **fields: Any) -> Self:
        return cls(kind="event", type=type, producer=producer, data=data, **fields)

    @classmethod
    def command(cls, type: str, producer: str, data: dict[str, Any], **fields: Any) -> Self:
        return cls(kind="command", type=type, producer=producer, data=data, **fields)

    def caused(self, type: str, producer: str, data: dict[str, Any], **fields: Any) -> Self:
        """Следствие этого сообщения: тот же сценарий, причина — это сообщение."""
        fields.setdefault("kind", "event")
        fields.setdefault("project_id", self.project_id)
        fields.setdefault("lifecycle_version", self.lifecycle_version)
        return self.__class__(
            type=type,
            producer=producer,
            data=data,
            correlation_id=self.correlation_id or self.message_id,
            causation_id=self.message_id,
            **fields,
        )

    def payload_hash(self) -> str:
        """SHA-256 канонического JSON: тот же `message_id` с другим хэшем — конфликт inbox."""
        canonical = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(canonical.encode()).hexdigest()
