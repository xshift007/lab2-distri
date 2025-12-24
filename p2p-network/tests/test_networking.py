import socket
import threading
import time

import pytest

from src import networking


@pytest.fixture(scope="module", autouse=True)
def servidor_tcp():
    hilo = threading.Thread(
        target=networking.iniciar_servidor,
        args=("127.0.0.1", 5000),
        daemon=True,
    )
    hilo.start()

    deadline = time.time() + 2
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 5000), timeout=0.1):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("El servidor no se inició a tiempo")

    yield


def test_enviar_mensaje_retorna_eco(servidor_tcp):
    mensaje = {"hola": "mundo"}
    respuesta = networking.enviar_mensaje("127.0.0.1", 5000, mensaje)

    assert respuesta == mensaje


def test_enviar_mensaje_retorna_error_para_texto(servidor_tcp):
    respuesta = networking.enviar_mensaje("127.0.0.1", 5000, "contenido no json")

    assert respuesta == {"error": "JSON invalido"}
