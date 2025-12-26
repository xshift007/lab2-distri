import time
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage import (
    Node,
    calcular_hash,
    add_node,
    find_successor,
    put_value,
    get_value,
    flood_search,
)


def build_ring(addresses: list[tuple[str, int]]) -> list[Node]:
    """Crea un anillo a partir de una lista de tuplas (ip, puerto).

    La lista devuelta está ordenada por ID de nodo y tiene los punteros de sucesor y
    predecesor configurados apropiadamente para cada nodo.
    """
    ring: list[Node] = []
    for ip, port in addresses:
        node = Node(ip, port)
        add_node(ring, node)
    return ring


def test_replication_and_lookup():
    """Los valores se almacenan en los nodos correctos y se replican.

    Esta prueba construye un anillo de tres nodos, almacena una clave y
    verifica que el valor esté presente en el nodo primario y en un
    sucesor (para un factor de replicación de 2). Luego verifica que
    ``get_value`` retrieves the correct value, regardless of which
    node originally stored it.
    """
    ring = build_ring([
        ("127.0.0.1", 5000),
        ("127.0.0.1", 5001),
        ("127.0.0.1", 5002),
    ])
    # Put a key/value pair with replication factor 2.
    key = "alumno1"
    value = "nota=6.0"
    put_value(ring, key, value, replication=2)
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    # Determine the indices of the primary and its successor.
    idx = ring.index(primary)
    replica_nodes = [ring[idx], ring[(idx + 1) % len(ring)]]
    # Ensure both replica nodes contain the key.
    for node in replica_nodes:
        assert key in node.data and node.data[key] == value
    # Nodes beyond the replication factor should not contain the key.
    non_replicas = [ring[(idx + 2) % len(ring)]]
    for node in non_replicas:
        assert key not in node.data
    # Retrieve the value via the ring's lookup helper.
    assert get_value(ring, key) == value


def test_flood_search_finds_value_within_ttl():
    """La búsqueda por inundación devuelve el valor cuando el TTL es suficiente.

    La clave se almacena en un nodo y se inicia una búsqueda por inundación desde
    otro nodo. Con TTL >= distancia, la búsqueda debería encontrar el
    valor. Con TTL demasiado bajo, debería devolver ``None``.
    """
    ring = build_ring([
        ("127.0.0.1", 5000),
        ("127.0.0.1", 5001),
        ("127.0.0.1", 5002),
    ])
    key = "important_key"
    value = "the_value"
    # Store on one node only (replication factor 1 for clarity).
    put_value(ring, key, value, replication=1)
    # Locate the node that holds the key.
    key_holder = find_successor(ring, calcular_hash(key))
    # Choose a different node to start the search.
    start_node = next(n for n in ring if n is not key_holder)
    # TTL of 2 is enough to reach the key in a three node ring.
    assert flood_search(start_node, key, ttl=2) == value
    # TTL of 0 means the search does not propagate beyond the start node.
    assert flood_search(start_node, key, ttl=0) is None


def test_heartbeat_detection():
    """Los nodos detectan vecinos fallidos cuando los latidos expiran.

    Un nodo recibe latidos de dos vecinos en tiempos controlados.
    Después de avanzar el reloj más allá de un umbral de tiempo de espera
    para un vecino, ``check_failed_neighbours`` debería devolver ese
    vecino como fallido mientras mantiene al otro.
    """
    a = Node("127.0.0.1", 5000)
    b = Node("127.0.0.1", 5001)
    c = Node("127.0.0.1", 5002)
    # Record heartbeats at specific times.
    base = 1000.0
    a.receive_heartbeat(b, timestamp=base)
    a.receive_heartbeat(c, timestamp=base)
    # After timeout/2 seconds, nothing should be considered failed.
    timeout = 10.0
    assert a.check_failed_neighbours(timeout, current_time=base + timeout / 2) == []
    # Advance time so that one neighbour's heartbeat is stale.
    # Provide a new heartbeat for c to keep it alive.
    a.receive_heartbeat(c, timestamp=base + timeout + 1)
    failed = a.check_failed_neighbours(timeout, current_time=base + timeout + 2)
    assert b in failed and c not in failed