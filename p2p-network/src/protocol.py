import json
import time
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

logger = logging.getLogger(__name__)

class MessageType(Enum):
    """Define los tipos de mensajes permitidos en la red P2P."""
    JOIN = "JOIN"
    UPDATE = "UPDATE"   
    PUT = "PUT"
    GET = "GET"
    RESULT = "RESULT"
    HEARTBEAT = "HEARTBEAT"

@dataclass
class Message:
    """Estructura de datos para los mensajes del protocolo."""
    type: MessageType
    sender_id: str
    data: Dict[str, Any]
    timestamp: float = None

    def __post_init__(self):
        # Asigna el tiempo actual si no se provee uno
        if self.timestamp is None:
            self.timestamp = time.time()

def serialize_message(message: Message) -> str:
    """Convierte una instancia de Message a una cadena JSON."""
    if not isinstance(message, Message):
        raise TypeError("Se esperaba una instancia de Message")

    payload = {
        "type": message.type.value,
        "sender_id": message.sender_id,
        "data": message.data,
        "timestamp": message.timestamp
    }
    return json.dumps(payload)

def deserialize_message(json_str: str) -> Message:
    """Convierte una cadena JSON a una instancia de Message con validación."""
    try:
        payload = json.loads(json_str)
    except json.JSONDecodeError:
        raise ValueError("Formato JSON inválido")

    # Validación de campos estructurales obligatorios
    required = {"type", "sender_id", "data", "timestamp"}
    if not required.issubset(payload):
        raise ValueError(f"Faltan campos obligatorios: {required - set(payload)}")

    try:
        msg_type = MessageType(payload["type"])
    except ValueError:
        raise ValueError(f"Tipo de mensaje desconocido: {payload['type']}")

    # Validación de contenido específico según el tipo
    _validate_payload_content(msg_type, payload["data"])

    return Message(
        type=msg_type,
        sender_id=str(payload["sender_id"]),
        data=payload["data"],
        timestamp=payload["timestamp"]
    )

def _validate_payload_content(msg_type: MessageType, data: Dict[str, Any]):
    """Verifica que el diccionario 'data' contenga la información necesaria para cada comando."""
    if msg_type == MessageType.JOIN:
        if "ip" not in data or "port" not in data:
            raise ValueError("JOIN requiere 'ip' y 'port'")
    elif msg_type == MessageType.PUT:
        if "key" not in data or "value" not in data:
            raise ValueError("PUT requiere 'key' y 'value'")
    elif msg_type == MessageType.GET:
        if "key" not in data:
            raise ValueError("GET requiere 'key'")