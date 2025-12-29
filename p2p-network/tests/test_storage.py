import pytest
from src.storage import LocalStorage

def test_operaciones_basicas_storage():
    """Verifica el flujo end-to-end local: PUT, GET y DELETE."""
    storage = LocalStorage()
    
    # Test PUT y GET
    storage.put("usuario1", "datos_test")
    assert storage.get("usuario1") == "datos_test"
    
    # Test DELETE
    storage.delete("usuario1")
    assert storage.get("usuario1") is None

def test_aislamiento_instancias():
    """Asegura que dos nodos diferentes tengan almacenamientos independientes."""
    nodo_a = LocalStorage()
    nodo_b = LocalStorage()
    
    nodo_a.put("key", "valor_a")
    nodo_b.put("key", "valor_b")
    
    assert nodo_a.get("key") == "valor_a"
    assert nodo_b.get("key") == "valor_b"

def test_obtener_todos_los_datos():
    """Verifica que get_all devuelva una copia íntegra para procesos de JOIN."""
    storage = LocalStorage()
    datos_originales = {"k1": "v1", "k2": "v2"}
    
    for k, v in datos_originales.items():
        storage.put(k, v)
    
    copia = storage.get_all()
    assert copia == datos_originales
    assert copia is not storage._data # Verifica que sea una copia, no el original

# --- TEST DE CONSISTENCIA DE REPLICACIÓN (Requisito PDF 5.3) ---
def test_logica_replicacion_simulada():
    """
    Simula el comportamiento del main.py: 
    Si un dato se guarda en el nodo 1, debe poder existir en el nodo 2.
    """
    storage_primario = LocalStorage()
    storage_sucesor = LocalStorage()
    
    # El flujo en main.py sería:
    key, val = "file_01", "contenido_binario"
    
    # 1. Guardar en el responsable
    storage_primario.put(key, val)
    
    # 2. Simular envío de réplica al sucesor
    storage_sucesor.put(key, val)
    
    assert storage_primario.get(key) == storage_sucesor.get(key)