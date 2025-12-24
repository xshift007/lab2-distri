import json
import logging
import socket
import threading
from typing import Any, Union


logger = logging.getLogger(__name__)


def manejar_cliente(conn: socket.socket, direccion) -> None:
    """
    Maneja una conexión con un cliente.

    Lee líneas separadas por '\\n', intenta decodificar cada una como JSON y envía
    un eco de la información recibida. Si el contenido no es JSON válido, envía un
    mensaje de error. Cierra la conexión al finalizar.
    """
    try:
        with conn:
            archivo = conn.makefile("r")
            for linea in archivo:
                linea = linea.strip()
                if not linea:
                    continue

                try:
                    datos = json.loads(linea)
                    respuesta = datos
                except json.JSONDecodeError:
                    respuesta = {"error": "JSON invalido"}

                try:
                    mensaje = json.dumps(respuesta) + "\n"
                except (TypeError, ValueError) as exc:
                    logger.error("Error serializando respuesta para %s: %s", direccion, exc)
                    mensaje = json.dumps({"error": "JSON invalido"}) + "\n"

                conn.sendall(mensaje.encode("utf-8"))
    finally:
        try:
            conn.close()
        except OSError:
            pass


def iniciar_servidor(host: str = "0.0.0.0", puerto: int = 5000) -> None:
    """
    Inicia un servidor TCP que acepta conexiones entrantes.

    Por cada cliente aceptado crea un hilo daemon que llama a `manejar_cliente`.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind((host, puerto))
        servidor.listen()
        logger.info("Servidor escuchando en %s:%s", host, puerto)

        while True:
            conn, addr = servidor.accept()
            logger.info("Conexión aceptada desde %s", addr)
            hilo = threading.Thread(target=manejar_cliente, args=(conn, addr), daemon=True)
            hilo.start()


def enviar_mensaje(host: str, puerto: int, mensaje_json: Union[dict, list, str]) -> Any:
    """
    Envía un mensaje JSON a un host y puerto específicos.

    Convierte diccionarios o listas a JSON, envía el contenido terminado en '\\n' y
    espera una respuesta del servidor. La respuesta se decodifica como JSON si es
    posible; de lo contrario, se devuelve la cadena recibida.
    """
    if isinstance(mensaje_json, (dict, list)):
        mensaje = json.dumps(mensaje_json)
    elif isinstance(mensaje_json, str):
        mensaje = mensaje_json
    else:
        raise TypeError("mensaje_json debe ser dict, list o str")

    with socket.create_connection((host, puerto)) as sock:
        sock.sendall((mensaje + "\n").encode("utf-8"))
        archivo = sock.makefile("r")
        respuesta = archivo.readline()

    respuesta = respuesta.strip()
    if not respuesta:
        return ""

    try:
        return json.loads(respuesta)
    except json.JSONDecodeError:
        return respuesta
