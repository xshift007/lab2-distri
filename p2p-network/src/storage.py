
from __future__ import annotations
import hashlib
import time
from typing import Dict, List, Optional, Set


def calcular_hash(value: str, bits: int = 32) -> int:
    """Devuelve un hash SHA-1 truncado de `value` como un entero.

    El digest completo de SHA-1 tiene 160 bits. Para simplificar y facilitar
    las pruebas, se trunca a `bits` bits (por defecto 32). El resultado es
    determinista para una entrada y cantidad de bits dada.
    """
    if not (1 <= bits <= 160):
        raise ValueError("bits debe estar entre 1 y 160")
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    full_int = int(digest, 16)
    if bits == 160:
        return full_int
    shift = 160 - bits
    return full_int >> shift


class Node:
    """Un nodo en el anillo hash simplificado.

    Cada nodo tiene una IP y puerto que juntos determinan su ID con `calcular_hash`.
    Mantiene punteros a su sucesor y predecesor, una tabla de datos, y
    registros de últimos latidos (heartbeats) de sus vecinos.
    """

    def __init__(self, ip: str, port: int) -> None:
        self.ip: str = ip
        self.port: int = port
        self.node_id: int = calcular_hash(f"{ip}:{port}")
        self.successor: Node = self
        self.predecessor: Node = self
        self.data: Dict[str, str] = {}
        self._last_heartbeat: Dict[Node, float] = {}

    def __repr__(self) -> str:
        return f"Node({self.ip}:{self.port}, id={self.node_id})"

    def receive_heartbeat(self, sender: Node, timestamp: Optional[float] = None) -> None:
        """Registra un latido recibido desde `sender`."""
        if timestamp is None:
            timestamp = time.time()
        self._last_heartbeat[sender] = timestamp

    def check_failed_neighbours(self, timeout: float, current_time: Optional[float] = None) -> List[Node]:
        """Devuelve una lista de vecinos cuyo latido ha expirado."""
        if current_time is None:
            current_time = time.time()
        failed: List[Node] = []
        for neighbour, last in list(self._last_heartbeat.items()):
            if current_time - last > timeout:
                failed.append(neighbour)
        return failed


def add_node(ring: List[Node], node: Node) -> None:
    """Agrega un nodo al anillo, manteniendo el orden y actualizando punteros."""
    if not ring:
        ring.append(node)
        node.successor = node
        node.predecessor = node
        return
    index = 0
    while index < len(ring) and ring[index].node_id < node.node_id:
        index += 1
    ring.insert(index, node)
    succ = ring[(index + 1) % len(ring)]
    pred = ring[(index - 1) % len(ring)]
    node.successor = succ
    node.predecessor = pred
    pred.successor = node
    succ.predecessor = node


def find_successor(ring: List[Node], key_id: int) -> Node:
    """Devuelve el nodo responsable de la clave dada por `key_id`."""
    if not ring:
        raise ValueError("ring está vacío")
    for node in ring:
        if key_id <= node.node_id:
            return node
    return ring[0]


def put_value(ring: List[Node], key: str, value: str, replication: int = 2) -> None:
    """Almacena el valor `value` bajo la clave `key` con replicación."""
    if replication < 1:
        raise ValueError("replication debe ser >= 1")
    if not ring:
        raise ValueError("ring está vacío")
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    index = ring.index(primary)
    total = min(replication, len(ring))
    for i in range(total):
        target = ring[(index + i) % len(ring)]
        target.data[key] = value


def get_value(ring: List[Node], key: str) -> Optional[str]:
    """Recupera el valor de una clave recorriendo el anillo desde el nodo primario."""
    if not ring:
        return None
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    index = ring.index(primary)
    for i in range(len(ring)):
        node = ring[(index + i) % len(ring)]
        if key in node.data:
            return node.data[key]
    return None


def flood_search(start: Node, key: str, ttl: int) -> Optional[str]:
    """Realiza una búsqueda por inundación a partir de un nodo con límite de saltos (TTL)."""
    if ttl < 0:
        raise ValueError("ttl debe ser >= 0")
    visited: Set[Node] = set()

    def _search(node: Node, remaining: int) -> Optional[str]:
        if key in node.data:
            return node.data[key]
        if remaining == 0:
            return None
        visited.add(node)
        for neighbour in (node.successor, node.predecessor):
            if neighbour not in visited:
                result = _search(neighbour, remaining - 1)
                if result is not None:
                    return result
        return None

    return _search(start, ttl)
