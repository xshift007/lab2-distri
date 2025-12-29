import socket
import threading
import time
import pytest
from src.storage import LocalStorage
from src.overlay import OverlayManager
from src import networking

# --- TEST DE ALMACENAMIENTO (Componente Básico) ---
def test_smoke_storage_init_and_operations():
    """Verifica inicialización correcta y flujo básico PUT/GET local[cite: 244, 245]."""
    storage = LocalStorage()
    
    # Verificar estado inicial vacío
    assert storage.get_all() == {}
    
    # Verificar operación de escritura
    storage.put("smoke_key", "humo")
    val = storage.get("smoke_key")
    
    assert val == "humo"
    
    # Verificar borrado
    storage.delete("smoke_key")
    assert storage.get("smoke_key") is None

# --- TEST DE OVERLAY (Componente Lógico) ---
def test_smoke_overlay_init():
    """Verifica que el nodo Overlay se inicialice con un ID válido."""
    overlay = OverlayManager("127.0.0.1", 8000)
    
    # El ID debe ser un entero (hash SHA-1)
    assert isinstance(overlay.node_id, int)
    # Inicialmente el sucesor debe ser él mismo
    assert overlay.successor["id"] == overlay.node_id

# --- TEST DE NETWORKING (Componente de Red) ---
def test_smoke_server_bind():
    """Verifica que el servidor TCP pueda hacer bind a un puerto real."""
    port = 5555
    
    # Callback dummy que no hace nada
    def dummy_callback(msg, addr): pass
    
    # Iniciamos servidor
    server_thread = networking.iniciar_servidor("127.0.0.1", port, dummy_callback)
    time.sleep(0.5) # Dar tiempo para levantar
    
    # Intentamos conectar realmente con un socket
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=1)
        connected = True
        sock.close()
    except Exception:
        connected = False
        
    assert connected is True, "El servidor TCP no aceptó conexiones en el puerto 5555"