import pytest
from src.protocol import (
    Message,
    MessageType,
    serialize_message,
    deserialize_message
)


def test_message_serialization_and_deserialization():
    msg = Message(
        type=MessageType.JOIN,
        sender_id="node1",
        data={"ip": "192.168.1.10", "port": 5000}
    )

    json_str = serialize_message(msg)
    assert isinstance(json_str, str)

    new_msg = deserialize_message(json_str)
    assert new_msg.type == MessageType.JOIN
    assert new_msg.sender_id == "node1"
    assert new_msg.data["ip"] == "192.168.1.10"


def test_invalid_json():
    with pytest.raises(ValueError):
        deserialize_message("not a json")


def test_missing_fields():
    bad_json = '{"type": "JOIN"}'
    with pytest.raises(ValueError):
        deserialize_message(bad_json)


def test_invalid_type():
    bad_json = '{"type":"BAD","sender_id":"x","data":{},"timestamp":1}'
    with pytest.raises(ValueError):
        deserialize_message(bad_json)


def test_heartbeat_message():
    msg = Message(
        type=MessageType.HEARTBEAT,
        sender_id="node1",
        data={}
    )

    json_msg = serialize_message(msg)
    parsed = deserialize_message(json_msg)

    assert parsed.type == MessageType.HEARTBEAT
    assert parsed.sender_id == "node1"
