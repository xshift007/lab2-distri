import json
import time
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

logger = logging.getLogger(__name__)

class MessageType(Enum):
    JOIN = "JOIN"
    UPDATE = "UPDATE"
    PUT = "PUT"
    GET = "GET"
    RESULT = "RESULT"
    HEARTBEAT = "HEARTBEAT"


@dataclass
class Message:
    type: MessageType
    sender_id: str
    data: Dict[str, Any]
    timestamp: float = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()



def serialize_message(message: Message) -> str:
    if not isinstance(message, Message):
        raise TypeError("Expected Message instance")

    payload = {
        "type": message.type.value,
        "sender_id": message.sender_id,
        "data": message.data,
        "timestamp": message.timestamp
    }

    return json.dumps(payload)

def deserialize_message(json_str: str) -> Message:
    try:
        payload = json.loads(json_str)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON")

    required_fields = {"type", "sender_id", "data", "timestamp"}
    if not required_fields.issubset(payload):
        raise ValueError("Missing required fields")

    try:
        msg_type = MessageType(payload["type"])
    except ValueError:
        raise ValueError("Invalid message type")

    if not isinstance(payload["sender_id"], str):
        raise ValueError("sender_id must be string")

    if not isinstance(payload["data"], dict):
        raise ValueError("data must be dict")

    if not isinstance(payload["timestamp"], (int, float)):
        raise ValueError("timestamp must be numeric")
    
    _validate_payload_content(msg_type, payload["data"])

    return Message(
        type=msg_type,
        sender_id=payload["sender_id"],
        data=payload["data"],
        timestamp=payload["timestamp"]
    )

def _validate_payload_content(msg_type: MessageType, data: Dict[str, Any]):
    """Valida que 'data' tenga los campos necesarios según el tipo"""
    if msg_type == MessageType.JOIN:
        if "ip" not in data or "port" not in data:
            raise ValueError("JOIN message missing 'ip' or 'port'")
    elif msg_type == MessageType.PUT:
        if "key" not in data or "value" not in data:
            raise ValueError("PUT message missing 'key' or 'value'")
    elif msg_type == MessageType.GET:
        if "key" not in data:
            raise ValueError("GET message missing 'key'")