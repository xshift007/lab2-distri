import json
import logging
import socket
import threading
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Callback global y referencia al socket del servidor para permitir cierres limpios
ON_MESSAGE_CALLBACK: Optional[Callable[[Dict, tuple], None]] = None
SERVER_SOCKET: Optional[socket.socket] = None

def manejar_cliente(conn: socket.socket, addr: tuple) -> None:
    """Gestiona la recepción de datos de un cliente, decodifica el JSON y ejecuta el callback."""
    try:
        with conn:
            data = conn.recv(4096).decode("utf-8")
            if not data:
                return
            try:
                mensaje_dict = json.loads(data)
                if ON_MESSAGE_CALLBACK:
                    ON_MESSAGE_CALLBACK(mensaje_dict, addr)
            except json.JSONDecodeError:
                logger.error(f"Error de protocolo: JSON inválido desde {addr}")
    except Exception as e:
        logger.error(f"Error en la conexión con {addr}: {e}")

def iniciar_servidor(host: str, puerto: int, callback: Callable) -> threading.Thread:
    """Inicializa el servidor TCP y configura el callback para procesar mensajes entrantes."""
    global ON_MESSAGE_CALLBACK, SERVER_SOCKET
    ON_MESSAGE_CALLBACK = callback

    SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    SERVER_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    SERVER_SOCKET.bind((host, puerto))
    SERVER_SOCKET.listen(5)
    
    def run_server():
        logger.info(f"Servicio de red iniciado en {host}:{puerto}")
        try:
            while True:
                # El bucle termina cuando SERVER_SOCKET.close() se llama desde fuera
                conn, addr = SERVER_SOCKET.accept()
                threading.Thread(target=manejar_cliente, args=(conn, addr), daemon=True).start()
        except OSError:
            logger.info("Servidor detenido correctamente.")

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    return server_thread

def enviar_mensaje(ip: str, puerto: int, mensaje_json: str) -> bool:
    """Envía una cadena JSON a un destino remoto y confirma si la entrega fue exitosa."""
    try:
        with socket.create_connection((ip, puerto), timeout=3) as sock:
            sock.sendall(mensaje_json.encode("utf-8"))
            return True
    except Exception as e:
        logger.error(f"Fallo de envío a {ip}:{puerto}: {e}")
        return False