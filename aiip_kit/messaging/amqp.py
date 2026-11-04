from collections.abc import Mapping

import aio_pika

from aiip_kit.messaging.message import MessageV1

ATTEMPT_HEADER = "x-attempt"


def to_amqp(message: MessageV1, headers: Mapping[str, str] | None = None) -> aio_pika.Message:
    """AMQP-сообщение из конверта: `message_id` и `type` совпадают с телом, доставка persistent."""
    return amqp_message(message.model_dump_json().encode(), message, headers)


def amqp_message(
    body: bytes, message: MessageV1, headers: Mapping[str, str] | None = None
) -> aio_pika.Message:
    return aio_pika.Message(
        body,
        message_id=str(message.message_id),
        type=message.type,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        timestamp=message.occurred_at,
        headers=dict(headers or {}),
    )


def from_amqp(body: bytes) -> MessageV1:
    """Конверт из тела сообщения.

    Raises:
        pydantic.ValidationError: тело не `MessageV1` — такое сообщение идёт в dead.
    """
    return MessageV1.model_validate_json(body)
