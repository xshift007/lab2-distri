import pytest
from src.overlay import OverlayManager

def test_hash_consistency():
    om = OverlayManager("127.0.0.1", 5000)
    hash1 = om.get_hash("test_key")
    hash2 = om.get_hash("test_key")
    assert hash1 == hash2
    assert isinstance(hash1, int)

def test_node_id_generation():
    om = OverlayManager("192.168.1.10", 8000)
    expected_id = om.get_hash("192.168.1.10:8000")
    assert om.node_id == expected_id

def test_is_responsible_simple():
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 100
    om.update_predecessor(50, "127.0.0.1", 4999)
    assert om.is_responsible(75) is True
    assert om.is_responsible(100) is True
    assert om.is_responsible(50) is False

def test_is_responsible_ring_wrap():
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 10
    om.update_predecessor(200, "127.0.0.1", 6000)
    assert om.is_responsible(205) is True
    assert om.is_responsible(5) is True
    assert om.is_responsible(15) is False

def test_update_neighbors():
    om = OverlayManager("127.0.0.1", 5000)
    om.update_successor(200, "192.168.1.5", 5000)
    assert om.successor["id"] == 200

def test_process_stabilize_response_better_successor():
    """Verifica que el nodo actualice su sucesor si encuentra uno más cercano."""
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 100
    # Sucesor actual lejos (ID 200)
    om.update_successor(200, "127.0.0.1", 5001)
    
    # Recibimos un candidato (ID 150) que está entre nosotros (100) y el sucesor (200)
    om.process_stabilize_response(150, "127.0.0.1", 5002)
    
    assert om.successor["id"] == 150
    assert om.successor["port"] == 5002

def test_notify_updates_predecessor():
    """Verifica que un nodo acepte a un nuevo predecesor si es mejor que el actual."""
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 100
    # Predecesor actual (ID 50)
    om.update_predecessor(50, "127.0.0.1", 4999)
    
    # Un nodo con ID 80 nos notifica que él es nuestro predecesor
    # Como 80 está entre 50 y 100, es un "mejor" predecesor.
    was_accepted = om.notify(80, "127.0.0.1", 5003)
    
    assert was_accepted is True
    assert om.predecessor["id"] == 80

def test_notify_rejects_worse_predecessor():
    """Verifica que un nodo rechace a un predecesor que no está en su rango."""
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 100
    om.update_predecessor(80, "127.0.0.1", 5003)
    
    # Un nodo con ID 40 intenta notificarnos, pero 80 ya es un mejor predecesor (está más cerca)
    was_accepted = om.notify(40, "127.0.0.1", 5004)
    
    assert was_accepted is False
    assert om.predecessor["id"] == 80