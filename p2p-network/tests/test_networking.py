import threading
import time

import pytest

from src import networking


@pytest.fixture(scope="module")
def mensajes_recibidos():
    return []


@pytest.fixture(scope="module", autouse=True)
def servidor(mensajes_recibidos):
    def handler(mensaje):
        mensajes_recibidos.append(mensaje)
        return mensaje

    hilo = threading.Thread(
        target=networking.iniciar_servidor,
        args=("127.0.0.1", 5000, handler),
        daemon=True,
    )
    hilo.start()
    time.sleep(0.1)
    return hilo


def test_enviar_mensaje_echo(mensajes_recibidos):
    mensaje = {"hola": "mundo"}

    respuesta = networking.enviar_mensaje("127.0.0.1", 5000, mensaje)

    assert mensajes_recibidos == [mensaje]
    assert respuesta == mensaje


def test_enviar_mensaje_json_invalido():
    respuesta = networking.enviar_mensaje("127.0.0.1", 5000, "no-json")

    assert respuesta == {"error": "JSON invalido"}
