import pytest
from src.overlay import OverlayManager

def test_responsabilidad_rango_circular():
    """Verifica que el nodo identifique correctamente sus llaves, incluso en el salto de anillo."""
    om = OverlayManager("127.0.0.1", 5000)
    
    # Caso 1: Rango normal (Predecesor 50, Nodo 100) -> Responsable de (50, 100]
    om.node_id = 100
    om.update_predecessor(50, "127.0.0.1", 4999)
    assert om.is_responsible(75) is True
    assert om.is_responsible(101) is False

    # Caso 2: Salto de anillo (Predecesor 900, Nodo 100) -> Responsable de (900, MAX] y [0, 100]
    om.node_id = 100
    om.update_predecessor(900, "127.0.0.1", 4999)
    assert om.is_responsible(950) is True  # Después del predecesor
    assert om.is_responsible(50) is True   # Después del cruce por cero
    assert om.is_responsible(150) is False # Fuera de rango

def test_estabilizacion_mejor_sucesor():
    """Verifica que el nodo adopte un sucesor más cercano si se descubre en la red."""
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 100
    om.update_successor(300, "127.0.0.1", 5001) # Sucesor lejano
    
    # Se descubre nodo 200 que está entre 100 y 300
    om.process_stabilize_response(200, "127.0.0.1", 5002)
    assert om.successor["id"] == 200

def test_notificacion_predecesor():
    """Valida la aceptación o rechazo de nuevos predecesores según cercanía."""
    om = OverlayManager("127.0.0.1", 5000)
    om.node_id = 500
    om.update_predecessor(100, "127.0.0.1", 4000)
    
    # Nodo 300 es mejor que 100 (está más cerca de 500)
    assert om.notify(300, "127.0.0.1", 3000) is True
    assert om.predecessor["id"] == 300
    
    # Nodo 200 es peor que 300
    assert om.notify(200, "127.0.0.1", 2000) is False
    assert om.predecessor["id"] == 300

def test_salida_graciosa_reset_variables():
    """Verifica el estado tras el manejo de fallos."""
    om = OverlayManager("127.0.0.1", 5000)
    om.update_successor(999, "127.0.0.1", 9999)
    om.handle_successor_failure()
    assert om.successor["id"] == om.node_id