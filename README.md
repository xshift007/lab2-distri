# Red P2P Distribuida (Chord-like)

Este proyecto implementa una red Peer-to-Peer funcional basada en una topología de anillo (Chord simplificado) con un sistema de almacenamiento distribuido, replicación de datos R=2 y mecanismos de auto-curación ante fallos.

## Instalación

Se utilizo Python 3.12.3
Instalar dependencias: pip install -r requirements.txt

## Guia de ejecución

Nodo Semilla: python3 main.py "puerto"
ejemplo: python3 main.py 5000

Nodos Adicionales (Unirse a la red): python3 main.py <puerto_propio> <IP_destino> <puerto_destino>
ejemplo: python3 main.py 5001 192.168.0.10 5000 (el nodo 5001 se crea y se conecta al nodo en el puerto 5000)

## Ejecución de pruebas

Ejecutar todas las pruebas: pytest tests/
Ejecutar pruebas de un modulo especıfico: pytest tests/test_protocol.py
Ejecutar con salida detallada: pytest tests/ -v
Ejecutar con cobertura de codigo: pytest tests/ --cov=src
