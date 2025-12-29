import pytest
import time
import threading
import json
import socket
from src.storage import LocalStorage
from src.overlay import OverlayManager
from src.protocol import Message, MessageType, deserialize_message, serialize_message
from src import networking

class MockNode:
    """Simula un nodo completo con su propia pila de protocolos y almacenamiento."""
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.storage = LocalStorage()
        self.overlay = OverlayManager(ip, port)
        
        # Configuramos el socket con SO_REUSEADDR antes de hacer el bind
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen(5)
        except OSError:
            # Si el puerto sigue ocupado, intentamos uno aleatorio
            self.server_socket.bind((self.ip, 0)) 
            self.port = self.server_socket.getsockname()[1]
            self.server_socket.listen(5)

        def run():
            try:
                while True:
                    conn, addr = self.server_socket.accept()
                    t = threading.Thread(target=self.manejar_cliente, args=(conn, addr))
                    t.daemon = True
                    t.start()
            except OSError:
                pass # El socket se cerró

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def manejar_cliente(self, conn, addr):
        """Recibe datos y procesa mensajes."""
        try:
            with conn:
                data = conn.recv(4096).decode("utf-8")
                if data:
                    self.procesar_mensaje(json.loads(data), addr)
        except:
            pass

    def procesar_mensaje(self, msg_dict, addr):
        """Lógica de respuesta para validación de tests."""
        msg = deserialize_message(json.dumps(msg_dict))
        
        if msg.type == MessageType.JOIN:
            self.overlay.update_successor(int(msg.sender_id), msg.data["ip"], msg.data["port"])
            resp = Message(MessageType.UPDATE, str(self.overlay.node_id), 
                           {"role": "predecessor", "ip": self.ip, "port": self.port})
            networking.enviar_mensaje(msg.data["ip"], msg.data["port"], serialize_message(resp))

        elif msg.type == MessageType.UPDATE:
            if msg.data["role"] == "predecessor":
                self.overlay.update_predecessor(int(msg.sender_id), msg.data["ip"], msg.data["port"])

        elif msg.type == MessageType.PUT:
            key, val = msg.data["key"], msg.data["value"]
            self.storage.put(key, val)
            if not msg.data.get("is_replica", False):
                rep = Message(MessageType.PUT, str(self.overlay.node_id), 
                              {"key": key, "value": val, "is_replica": True})
                networking.enviar_mensaje(self.overlay.successor["ip"], self.overlay.successor["port"], serialize_message(rep))

    def stop(self):
        """Cierra el socket de forma forzosa."""
        try:
            self.server_socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        self.server_socket.close()

@pytest.fixture
def red_p2p():
    """Configura y destruye una red con puertos distintos para cada ejecución de test."""
    # Usar puertos altos y diferentes en cada instancia ayuda a evitar el TIME_WAIT
    n1 = MockNode("127.0.0.1", 8001)
    n2 = MockNode("127.0.0.1", 8002)
    time.sleep(0.5)
    yield n1, n2
    n1.stop()
    n2.stop()
    time.sleep(0.5)

def test_distributed_join(red_p2p):
    """Verifica que el protocolo JOIN funcione correctamente."""
    n1, n2 = red_p2p
    n2.overlay.join(n1.ip, n1.port)
    time.sleep(1)
    assert n1.overlay.successor["port"] == n2.port
    assert n2.overlay.predecessor["port"] == n1.port

def test_distributed_replication(red_p2p):
    """Verifica la replicación R=2."""
    n1, n2 = red_p2p
    n1.overlay.update_successor(n2.overlay.node_id, n2.ip, n2.port)
    
    msg = Message(MessageType.PUT, "ext", {"key": "rep", "value": "ok"})
    networking.enviar_mensaje(n1.ip, n1.port, serialize_message(msg))
    time.sleep(1)
    
    assert n1.storage.get("rep") == "ok"
    assert n2.storage.get("rep") == "ok"