import pytest
from src.protocol import (
    Message,
    MessageType,
    serialize_message,
    deserialize_message
)

def test_ciclo_completo_serializacion():
    """Verifica que un mensaje se mantenga íntegro tras serializar y deserializar."""
    orig_msg = Message(
        type=MessageType.JOIN,
        sender_id="node_test_1",
        data={"ip": "127.0.0.1", "port": 8080}
    )
    
    encoded = serialize_message(orig_msg)
    decoded = deserialize_message(encoded)
    
    assert decoded.type == MessageType.JOIN
    assert decoded.sender_id == "node_test_1"
    assert decoded.data["port"] == 8080
    assert isinstance(decoded.timestamp, float)

def test_validacion_datos_especificos():
    """Verifica que falle si el JSON es válido pero faltan datos internos (ej. falta la key en un PUT)."""
    # Caso PUT sin valor
    bad_put = {
        "type": "PUT",
        "sender_id": "1",
        "data": {"key": "nombre"}, # Falta "value"
        "timestamp": 12345.6
    }
    with pytest.raises(ValueError, match="PUT requiere 'key' y 'value'"):
        deserialize_message(json.dumps(bad_put))

import json

def test_error_campos_estructurales_faltantes():
    """Verifica que falle si faltan campos base como 'sender_id'."""
    incomplete_json = '{"type": "HEARTBEAT", "data": {}}' # Falta sender_id y timestamp
    with pytest.raises(ValueError, match="Faltan campos obligatorios"):
        deserialize_message(incomplete_json)

def test_tipos_mensaje_validos():
    """Asegura que todos los tipos definidos en el Enum funcionen."""
    for m_type in MessageType:
        data = {}
        # Llenar datos mínimos para evitar errores de validación de contenido
        if m_type == MessageType.JOIN: data = {"ip": "x", "port": 1}
        elif m_type == MessageType.PUT: data = {"key": "k", "value": "v"}
        elif m_type == MessageType.GET: data = {"key": "k"}
        
        msg = Message(type=m_type, sender_id="id", data=data)
        assert deserialize_message(serialize_message(msg)).type == m_type