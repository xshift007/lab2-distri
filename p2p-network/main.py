import sys
import time
import threading
import logging
import json
import socket

from src import networking
from src.protocol import deserialize_message, Message, MessageType, serialize_message
from src.overlay import OverlayManager
from src.storage import LocalStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MAIN")

overlay = None
storage = None

def obtener_ip_local():
    """Detecta la IP real de la interfaz de red activa."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No necesita conexión real, solo para identificar la interfaz local
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def transferir_llaves(nuevo_nodo_id, nuevo_ip, nuevo_port):
    todas = storage.get_all()
    for k, v in todas.items():
        k_hash = overlay.get_hash(k)
        if overlay.is_responsible(k_hash, nuevo_nodo_id):
            logger.info(f"-> Transfiriendo llave '{k}' al nuevo nodo responsable.")
            msg = Message(MessageType.PUT, str(overlay.node_id), {"key": k, "value": v})
            networking.enviar_mensaje(nuevo_ip, nuevo_port, serialize_message(msg))

def procesar_mensaje(msg_dict, addr):
    try:
        msg = deserialize_message(json.dumps(msg_dict))
        
        if msg.type == MessageType.JOIN:
            new_id = int(msg.sender_id)
            new_ip, new_port = msg.data["ip"], msg.data["port"]
            logger.info(f"Procesando JOIN de nodo {new_ip}:{new_port}")
            
            if overlay.successor["id"] == overlay.node_id:
                overlay.update_successor(new_id, new_ip, new_port)
                overlay.update_predecessor(new_id, new_ip, new_port)
            else:
                overlay.update_successor(new_id, new_ip, new_port)

            transferir_llaves(new_id, new_ip, new_port)
            resp = Message(MessageType.UPDATE, str(overlay.node_id), 
                           {"role": "predecessor", "ip": overlay.ip, "port": overlay.port})
            networking.enviar_mensaje(new_ip, new_port, serialize_message(resp))

        elif msg.type == MessageType.PUT:
            key, val = msg.data["key"], msg.data["value"]
            is_replica = msg.data.get("is_replica", False)
            storage.put(key, val)
            logger.info(f"Dato guardado: {key} (Replica: {is_replica})")

            if not is_replica and overlay.successor["id"] != overlay.node_id:
                rep_msg = Message(MessageType.PUT, str(overlay.node_id), 
                                  {"key": key, "value": val, "is_replica": True})
                networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(rep_msg))

        elif msg.type == MessageType.GET:
            key = msg.data["key"]
            val = storage.get(key)
            if val:
                resp = Message(MessageType.RESULT, str(overlay.node_id), {"key": key, "value": val})
                networking.enviar_mensaje(msg.data["requester_ip"], msg.data["requester_port"], serialize_message(resp))
            else:
                if msg.data["requester_port"] != overlay.port or msg.data["requester_ip"] != overlay.ip:
                    networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(msg))

        elif msg.type == MessageType.HEARTBEAT:
            overlay.notify(int(msg.sender_id), msg.data["ip"], msg.data["port"])
            if overlay.successor["id"] == overlay.node_id:
                overlay.update_successor(int(msg.sender_id), msg.data["ip"], msg.data["port"])

        elif msg.type == MessageType.UPDATE:
            if msg.data["role"] == "predecessor":
                overlay.update_predecessor(int(msg.sender_id), msg.data["ip"], msg.data["port"])
            elif msg.data["role"] == "successor":
                overlay.update_successor(int(msg.sender_id), msg.data["ip"], msg.data["port"])
        
        elif msg.type == MessageType.RESULT:
            print(f"\n[RESULTADO] {msg.data['key']} = {msg.data['value']}")

    except Exception as e:
        logger.error(f"Error procesando mensaje: {e}")

def tareas_mantenimiento():
    while True:
        time.sleep(5)
        if overlay.successor["id"] != overlay.node_id:
            hb = Message(MessageType.HEARTBEAT, str(overlay.node_id), {"ip": overlay.ip, "port": overlay.port})
            if not networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(hb)):
                overlay.handle_successor_failure()

        if overlay.predecessor and (time.time() - overlay.last_heartbeat > 15):
            logger.warning("Timeout de predecesor detectado.")
            overlay.predecessor = None

def main():
    global overlay, storage
    if len(sys.argv) < 2:
        print("Uso: python main.py <PUERTO> [IP_BOOTSTRAP] [PUERTO_BOOTSTRAP]")
        sys.exit(1)

    mi_ip = obtener_ip_local()
    mi_puerto = int(sys.argv[1])
    
    bootstrap_ip = sys.argv[2] if len(sys.argv) > 2 else None
    bootstrap_port = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    storage = LocalStorage()
    overlay = OverlayManager(mi_ip, mi_puerto)
    
    networking.iniciar_servidor(mi_ip, mi_puerto, procesar_mensaje)
    
    if bootstrap_ip and bootstrap_port:
        overlay.join(bootstrap_ip, bootstrap_port)

    threading.Thread(target=tareas_mantenimiento, daemon=True).start()

    try:
        while True:
            print(f"\n--- NODO EN {mi_ip}:{mi_puerto} ---")
            print("1. Estado | 2. PUT | 3. GET | q. Salir")
            op = input("> ")
            if op == "1":
                print(f"ID: {overlay.node_id}")
                print(f"Sucesor: {overlay.successor.get('ip')}:{overlay.successor.get('port')}")
                print(f"Predecesor: {overlay.predecessor['ip'] + ':' + str(overlay.predecessor['port']) if overlay.predecessor else 'None'}")
                print(f"Data: {storage.get_all()}")
            elif op == "2":
                k, v = input("Clave: "), input("Valor: ")
                if overlay.is_responsible(overlay.get_hash(k)):
                    storage.put(k, v)
                    if overlay.successor["id"] != overlay.node_id:
                        msg = Message(MessageType.PUT, str(overlay.node_id), {"key": k, "value": v, "is_replica": True})
                        networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(msg))
                else:
                    msg = Message(MessageType.PUT, str(overlay.node_id), {"key": k, "value": v})
                    networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(msg))
            elif op == "3":
                k = input("Clave: ")
                val = storage.get(k)
                if val: print(f"Encontrado localmente: {val}")
                else:
                    msg = Message(MessageType.GET, str(overlay.node_id), {"key": k, "requester_ip": mi_ip, "requester_port": mi_puerto})
                    networking.enviar_mensaje(overlay.successor["ip"], overlay.successor["port"], serialize_message(msg))
            elif op == "q":
                break
    except KeyboardInterrupt:
        pass
    finally:
        overlay.leave()
        if networking.SERVER_SOCKET:
            networking.SERVER_SOCKET.close()
        print("\nNodo apagado y puerto liberado.")

if __name__ == "__main__":
    main()