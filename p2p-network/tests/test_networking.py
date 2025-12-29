import pytest
import time
import json
from src import networking

# Variable para capturar lo que el servidor recibe durante los tests
received_data = []

def mock_callback(msg, addr):
    """Callback de prueba para capturar mensajes entrantes."""
    received_data.append(msg)

@pytest.fixture(scope="module")
def servidor_test():
    """Levanta el servidor una sola vez para todos los tests del módulo."""
    host = "127.0.0.1"
    port = 5005
    networking.iniciar_servidor(host, port, mock_callback)
    time.sleep(0.5) # Tiempo para que el socket se enlace
    return host, port

def test_conexion_y_envio_exitoso(servidor_test):
    """Verifica que un JSON enviado sea recibido correctamente por el servidor."""
    host, port = servidor_test
    test_msg = {"test": "hola"}
    
    # Limpiamos datos previos
    received_data.clear()
    
    exito = networking.enviar_mensaje(host, port, json.dumps(test_msg))
    
    # Damos un pequeño margen para el procesamiento del hilo
    time.sleep(0.1)
    
    assert exito is True
    assert len(received_data) > 0
    assert received_data[0] == test_msg

def test_fallo_conexion_puerto_invalido():
    """Verifica que el sistema maneje correctamente un destino inexistente."""
    exito = networking.enviar_mensaje("127.0.0.1", 9999, '{"msg": "error"}')
    assert exito is False

def test_manejo_json_corrupto(servidor_test):
    """Verifica que el servidor no explote al recibir texto plano no JSON."""
    host, port = servidor_test
    received_data.clear()
    
    # Enviamos algo que no es JSON
    exito = networking.enviar_mensaje(host, port, "esto_no_es_json")
    
    time.sleep(0.1)
    assert exito is True # El envío es exitoso, aunque el parsing falle en el servidor
    assert len(received_data) == 0 # El callback no debió ejecutarse