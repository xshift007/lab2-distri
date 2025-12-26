import hashlib
import logging

logger = logging.getLogger(__name__)

class OverlayManager:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        # Generar ID del nodo basado en su dirección "IP:PORT"
        self.node_id = self.get_hash(f"{ip}:{port}")
        
        # En Chord inicial, el sucesor es uno mismo
        self.successor = {"id": self.node_id, "ip": self.ip, "port": self.port}
        self.predecessor = None
        
        logger.info(f"Nodo inicializado con ID: {self.node_id}")

    def get_hash(self, key: str) -> int:
        """Calcula el hash SHA-1 y lo convierte a entero para el anillo."""
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def is_responsible(self, target_id: int) -> bool:
        """Verifica si este nodo es responsable de un ID (hash) dado."""
        if self.predecessor is None:
            return True
        
        p_id = self.predecessor["id"]
        n_id = self.node_id
        
        if p_id < n_id:
            return p_id < target_id <= n_id
        else:
            return target_id > p_id or target_id <= n_id

    def update_successor(self, new_id: int, new_ip: str, new_port: int):
        self.successor = {"id": new_id, "ip": new_ip, "port": new_port}
        logger.info(f"Nuevo successor: {new_id}")

    def update_predecessor(self, new_id: int, new_ip: str, new_port: int):
        self.predecessor = {"id": new_id, "ip": new_ip, "port": new_port}
        logger.info(f"Nuevo predecessor: {new_id}")

    def join(self, bootstrap_ip: str, bootstrap_port: int):
        """
        Inicia el proceso de unión a una red existente.
        Nota: Esto requiere que el Módulo 1 (Networking) esté listo para enviar el mensaje.
        """
        logger.info(f"Intentando unirse a la red a través de {bootstrap_ip}:{bootstrap_port}")
        # Aquí se construiría un mensaje tipo JOIN usando el Módulo 2
        # y se enviaría usando el Módulo 1.
        pass

    def stabilize(self):
        """
        Operación periódica para verificar que el sucesor es correcto
        y notificarle que somos su predecesor.
        """
        if self.successor and self.successor["id"] != self.node_id:
            logger.debug("Ejecutando estabilización periódica...")
            # Lógica para preguntar al sucesor por su predecesor
            pass

    def get_stabilization_message(self):
        """
        Genera la consulta para el sucesor. 
        En el protocolo JSON, esto sería un mensaje tipo 'GET_PREDECESSOR'.
        """
        if self.successor:
            return {
                "target_ip": self.successor["ip"],
                "target_port": self.successor["port"],
                "type": "GET_PREDECESSOR"
            }
        return None

    def process_stabilize_response(self, candidate_id, candidate_ip, candidate_port):
        """
        Procesa la respuesta del sucesor.
        Si el sucesor tiene un predecesor (candidate) que está más cerca de nosotros,
        lo adoptamos como nuestro nuevo sucesor.
        """
        n_id = self.node_id
        s_id = self.successor["id"]

        # Si el candidato está entre nosotros y nuestro sucesor actual
        if candidate_id is not None:
            if n_id < candidate_id < s_id or (n_id > s_id and (candidate_id > n_id or candidate_id < s_id)):
                self.update_successor(candidate_id, candidate_ip, candidate_port)
                logger.info(f"Estabilización: Sucesor actualizado a {candidate_id}")

    def notify(self, potential_predecessor_id, ip, port):
        """
        Lógica cuando otro nodo nos dice 'Yo soy tu predecesor'.
        Solo aceptamos si es más cercano que nuestro predecesor actual o si no tenemos uno.
        """
        if self.predecessor is None or self.is_better_predecessor(potential_predecessor_id):
            self.update_predecessor(potential_predecessor_id, ip, port)
            return True
        return False

    def is_better_predecessor(self, p_id):
        """Determina si un ID es un mejor predecesor que el actual."""
        current_p_id = self.predecessor["id"]
        n_id = self.node_id
        
        if current_p_id < n_id:
            return current_p_id < p_id < n_id
        else: # Salto del anillo
            return p_id > current_p_id or p_id < n_id