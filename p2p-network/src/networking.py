import json
import logging
import socket
import threading
from typing import Any, Callable

logger = logging.getLogger(__name__)


def iniciar_servidor(host: str, port: int, manejar_mensaje: Callable[[Any], Any]) -> None:
    """Inicia un servidor TCP simple que procesa mensajes JSON.

    El servidor acepta conexiones entrantes, intenta decodificar el contenido
    como JSON y pasa el resultado al callback ``manejar_mensaje``. La respuesta
    se envía de vuelta serializada como JSON. Si el contenido recibido no es
    JSON válido, se responde con ``{"error": "JSON invalido"}``.
    """

    def _manejar_cliente(conn: socket.socket, addr) -> None:
        with conn:
            try:
                datos = conn.recv(4096)
            except OSError:
                return

            if not datos:
                return

            try:
                mensaje = json.loads(datos.decode("utf-8"))
            except json.JSONDecodeError:
                respuesta = {"error": "JSON invalido"}
            else:
                try:
                    respuesta = manejar_mensaje(mensaje)
                except Exception as exc:  # pragma: no cover - logging extra
                    logger.exception("Error al manejar mensaje desde %s", addr)
                    respuesta = {"error": str(exc)}

            _enviar_respuesta(conn, respuesta)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((host, port))
    servidor.listen()

    while True:
        try:
            conn, addr = servidor.accept()
        except OSError:
            break
        hilo = threading.Thread(target=_manejar_cliente, args=(conn, addr), daemon=True)
        hilo.start()


def enviar_mensaje(host: str, port: int, mensaje: Any) -> Any:
    """Envía un mensaje al servidor y devuelve la respuesta decodificada.

    Los diccionarios y otros tipos serializables se envían como JSON. Las
    cadenas se envían tal cual, lo que permite probar el manejo de datos que no
    sean JSON válidos.
    """

    if isinstance(mensaje, dict):
        datos = json.dumps(mensaje)
    elif isinstance(mensaje, str):
        datos = mensaje
    else:
        datos = json.dumps(mensaje)

    with socket.create_connection((host, port), timeout=1) as sock:
        sock.sendall(datos.encode("utf-8"))
        respuesta = sock.recv(4096)

    return json.loads(respuesta.decode("utf-8"))


def _enviar_respuesta(conn: socket.socket, respuesta: Any) -> None:
    try:
        serializado = json.dumps(respuesta)
    except TypeError:
        serializado = json.dumps({"error": "Respuesta no serializable"})

    conn.sendall(serializado.encode("utf-8"))
