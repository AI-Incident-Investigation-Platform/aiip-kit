import uuid

import aio_pika
import pytest
from pydantic import ValidationError

from aiip_kit.messaging.amqp import from_amqp, to_amqp
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_investigation


def make_event() -> MessageV1:
    return MessageV1.event(
        "incident.created",
        "incidents",
        {"incident_id": "42"},
        project_id=uuid.uuid4(),
        actor=Actor(kind="user", id="u-1"),
    )


def test_amqp_roundtrip_keeps_envelope() -> None:
    message = make_event()

    amqp = to_amqp(message, {"traceparent": "00-abc-def-01"})

    assert amqp.message_id == str(message.message_id)
    assert amqp.type == "incident.created"
    assert amqp.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert amqp.headers == {"traceparent": "00-abc-def-01"}
    assert from_amqp(amqp.body) == message


def test_payload_hash_depends_on_content_only() -> None:
    message = make_event()
    same = MessageV1.model_validate_json(message.model_dump_json())
    other = message.model_copy(update={"data": {"incident_id": "43"}})

    assert message.payload_hash() == same.payload_hash()
    assert message.payload_hash() != other.payload_hash()


def test_caused_message_continues_scenario() -> None:
    cause = make_event()

    effect = cause.caused("inbox.changed", "notifications", {"unread_count": 1})

    assert effect.causation_id == cause.message_id
    assert effect.correlation_id == cause.message_id
    assert effect.project_id == cause.project_id
    assert effect.message_id != cause.message_id


def test_unknown_field_is_rejected() -> None:
    body = make_event().model_dump(mode="json") | {"secret": "x"}

    with pytest.raises(ValidationError):
        MessageV1.model_validate(body)


def test_destination_json_roundtrip() -> None:
    project_id, investigation_id = uuid.uuid4(), uuid.uuid4()
    destination = Destination.live("incidents", live_investigation(project_id, investigation_id))

    assert Destination.from_json(destination.to_json()) == destination
    assert destination.exchange == "incidents.live"
    assert destination.mandatory is False
    assert Destination.command("incidents", "alert.received").exchange_type == "direct"
