import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class LocalStorage:
    """Gestiona el almacenamiento de pares clave-valor en la memoria del nodo."""
    
    def __init__(self):
        self._data: Dict[str, Any] = {}
        logger.info("Sistema de almacenamiento local listo.")

    def put(self, key: str, value: Any) -> None:
        """Inserta o actualiza una clave en el diccionario local."""
        self._data[key] = value

    def get(self, key: str) -> Optional[Any]:
        """Recupera el valor asociado a una clave. Retorna None si no existe."""
        return self._data.get(key)

    def delete(self, key: str) -> None:
        """Elimina una clave del almacenamiento local si existe."""
        if key in self._data:
            del self._data[key]

    def get_all(self) -> Dict[str, Any]:
        """Retorna una copia de todos los datos (utilizado para transferencias en JOIN)."""
        return self._data.copy()