import sys
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

import networking


if __name__ == "__main__":
    host = "0.0.0.0"
    port = 5000

    try:
        print(f"Iniciando servidor en {host}:{port}...")
        networking.iniciar_servidor(host, port)
    except KeyboardInterrupt:
        print("\nServidor detenido por el usuario.")
    except Exception as exc:
        print(f"Error al iniciar el servidor: {exc}")
