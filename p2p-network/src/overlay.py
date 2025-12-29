import hashlib
import logging
import time
from src import networking
from src.protocol import Message, MessageType, serialize_message

logger = logging.getLogger(__name__)

class OverlayManager:
    """Gestiona la topología de anillo hash y las relaciones entre nodos (Chord)."""
    
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.node_id = self.get_hash(f"{ip}:{port}")
        
        # Estado inicial: el nodo es su propio sucesor (Anillo de 1)
        self.successor = {"id": self.node_id, "ip": self.ip, "port": self.port}
        self.predecessor = None
        self.last_heartbeat = time.time()

        logger.info(f"Nodo Chord configurado - ID: {self.node_id}")

    def get_hash(self, key: str) -> int:
        """Calcula la posición de una clave en el anillo usando SHA-1 (160 bits)."""
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def is_responsible(self, key_hash: int, node_id: int = None) -> bool:
        """Determina si un nodo (por defecto este) es responsable de una llave."""
        current_id = node_id if node_id is not None else self.node_id
        
        # Si soy el único nodo o no tengo predecesor, soy responsable de todo
        if self.predecessor is None or self.predecessor["id"] == current_id:
            return True
            
        p_id = int(self.predecessor["id"])
        
        # Caso normal: Predecesor < Yo
        if p_id < current_id:
            return p_id < key_hash <= current_id
        # Caso de salto de anillo (Wrap-around): Yo < Predecesor
        else:
            return key_hash > p_id or key_hash <= current_id

    def update_successor(self, new_id: int, new_ip: str, new_port: int):
        """Actualiza la referencia al sucesor."""
        self.successor = {"id": new_id, "ip": new_ip, "port": new_port}
        logger.info(f"Sucesor actualizado -> {new_port} (ID: {str(new_id)[:8]}...)")

    def update_predecessor(self, new_id: int, new_ip: str, new_port: int):
        """Actualiza la referencia al predecesor y resetea el watchdog."""
        self.predecessor = {"id": new_id, "ip": new_ip, "port": new_port}
        self.last_heartbeat = time.time()
        logger.info(f"Predecesor actualizado -> {new_port} (ID: {str(new_id)[:8]}...)")

    def join(self, bootstrap_ip: str, bootstrap_port: int):
        """Solicita entrada a la red a través de un nodo conocido."""
        logger.info(f"Solicitando unión a red vía {bootstrap_ip}:{bootstrap_port}")
        msg = Message(
            type=MessageType.JOIN,
            sender_id=str(self.node_id),
            data={"ip": self.ip, "port": self.port}
        )
        networking.enviar_mensaje(bootstrap_ip, bootstrap_port, serialize_message(msg))

    def notify(self, potential_p_id: int, ip: str, port: int):
        """Responde a un Heartbeat actualizando el predecesor si corresponde."""
        # En Chord simple, aceptamos al que nos notifica como predecesor
        self.update_predecessor(potential_p_id, ip, port)

    def handle_successor_failure(self):
        """Lógica de recuperación: si el sucesor cae, nos apuntamos a nosotros mismos."""
        if self.successor["id"] != self.node_id:
            logger.warning("Fallo de sucesor detectado. Reconfigurando anillo...")
            # Vuelta al estado seguro: yo soy mi propio sucesor hasta nueva orden
            self.successor = {"id": self.node_id, "ip": self.ip, "port": self.port}

    def leave(self):
        """Notifica a los vecinos antes de cerrar para mantener la consistencia."""
        if self.successor["id"] != self.node_id and self.predecessor:
            # Avisar al sucesor que su nuevo predecesor es mi actual predecesor
            msg_s = Message(type=MessageType.UPDATE, sender_id=str(self.node_id),
                            data={"role": "predecessor", **self.predecessor})
            networking.enviar_mensaje(self.successor["ip"], self.successor["port"], serialize_message(msg_s))

            # Avisar al predecesor que su nuevo sucesor es mi actual sucesor
            msg_p = Message(type=MessageType.UPDATE, sender_id=str(self.node_id),
                            data={"role": "successor", **self.successor})
            networking.enviar_mensaje(self.predecessor["ip"], self.predecessor["port"], serialize_message(msg_p))