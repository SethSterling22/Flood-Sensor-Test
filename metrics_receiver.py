"""
Description very descriptive

"""

#import threading
#import random
#import signal
import os
import sys
import time
import json
import socket
import logging
import queue
import threading
from datetime import datetime
from dotenv import load_dotenv


# === ENVIRONMENT VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables
HOST = "0.0.0.0"
PORT = int(os.getenv("RECEIVER_PORT") or 4040)


# === GLOBAL VARIABLES ===
NODES = {}  # {node_id: {'conn': connection, 'addr': address, 'last_active': timestamp}}
NODE_QUEUE = queue.Queue()  # Cola FIFO para procesamiento ordenado
PROCESSING_LOCK = threading.Lock()
LOG_DIR = "./Logs/"

# Create Directory
os.makedirs(LOG_DIR, exist_ok=True)

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR,'metrics_receiver.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



# def start_server():
#     """
#     This function is responsible for synchronizing the nodes, 
#     receiving their information and sending it to "metrics_uploader.py" 
#     to be uploaded to Upstream-dso and create Subtask in Mint based on 
#     the Streamflow calculated with the functions of "utils.py".
#     """

#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:

#             # Make the port Reusable if it turns down
#             server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             server_socket.bind((HOST, PORT))
#             server_socket.listen()
#             print(f"🟢 Server actived on: {HOST}:{PORT}")
#             logger.info(f"🟢 Server actived on: {HOST}:{PORT}")

#             while True:
#                 conn, addr = server_socket.accept()
#                 with conn:
#                     print(f"📡 Connection stablished from: {addr}")
#                     logger.info(f"📡 Connection stablished from: {addr}")
#                     # Send signal to the client when connection is stablished
#                     conn.sendall(b"READY")

#                     # Wait for the client to sent the data (Must be pushed in a queue)
#                     data = conn.recv(4096)
#                     if not data:
#                         continue

#                     try:
#                         message = json.loads(data.decode("utf-8"))
#                         print(f"🕓 {datetime.now()} - Received Data:")
                        
#                         print(json.dumps(message, indent=4))
#                         logger.info(json.dumps(message, indent=4))
#                         conn.sendall(b"OK")  # Confirmación final
#                     except json.JSONDecodeError:
#                         print("❌ Error: Received Data is not a valid JSON format.")
#                         conn.sendall(b"ERROR")

#     except KeyboardInterrupt:
#         logger.info("📡 Metrics Receiver stopped by user.")
#     finally:
#         sys.exit(0)

# if __name__ == "__main__":
#     start_server()

##############################################################################################3



def handle_node_connection(conn, addr):
    """
    Manage each NODE individually and send
    send its data to process and upload
    """

    node_id = None
    try:
        # Step 1: Node identification
        conn.sendall(b"NODE_ID_REQUEST")
        node_id_response = conn.recv(1024).decode('utf-8').strip()
        
        if not node_id_response:
            raise ValueError("Empty node ID received")
        
        node_id = node_id_response
        with PROCESSING_LOCK:
            NODES[node_id] = {
                'conn': conn,
                'addr': addr,
                'last_active': time.time(),
                'status': 'connected'
            }
        
        logger.info(f"🆔 Node {node_id} connected from {addr}")
        conn.sendall(b"READY")

        # Step 2: Data retreival 
        while True:
            data = conn.recv(4096)
            if not data:
                break

            current_time = time.time()
            try:
                message = json.loads(data.decode('utf-8'))
                logger.info(f"📥 Received data from {node_id}: {message['thread']}")

                # Basic message validation
                if 'thread' not in message or 'data' not in message:
                    raise ValueError("Invalid message format")

                # Update current activity
                with PROCESSING_LOCK:
                    NODES[node_id]['last_active'] = current_time

                # Add to the processing queue
                NODE_QUEUE.put({
                    'node_id': node_id,
                    'message': message,
                    'timestamp': current_time
                })

                # Response
                conn.sendall(b"OK_QUEUED")

            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"❌ Error processing data from {node_id}: {str(e)}")
                conn.sendall(b"BAD_FORMAT")

    except ConnectionResetError:
        logger.warning(f"⚠️ Connection reset by node {node_id or addr}")
    except Exception as e:
        logger.error(f"🔴 Error with node {node_id or addr}: {str(e)}")
    finally:
        if node_id:
            with PROCESSING_LOCK:
                if node_id in NODES:
                    del NODES[node_id]
        conn.close()
        logger.info(f"🚪 Node {node_id or addr} disconnected")


def process_node_data():
    """
    Procesa los datos de los nodos en orden de llegada
    """
    
    while True:
        try:
            # Obtener el siguiente elemento de la cola (bloqueante)
            item = NODE_QUEUE.get()
            node_id = item['node_id']
            message = item['message']
            timestamp = item['timestamp']

            # PROCESAMIENTO!!! -> Send it to Upload
            # Simular procesamiento (aquí iría tu lógica real)
            logger.info(f"🔧 Processing {message['thread']} from {node_id}")
            time.sleep(1)  # Simula tiempo de procesamiento
            
            # Mark as completed
            NODE_QUEUE.task_done()
            logger.info(f"✅ Processed {message['thread']} from {node_id}")

        except Exception as e:
            logger.error(f"❌ Error processing data: {str(e)}")

def monitor_nodes():
    """
    Manage the interactive NODES and reject the ones with timeout
    """

    while True:
        time.sleep(5)  # Check out each 5 seconds
        current_time = time.time()
        inactive_nodes = []

        with PROCESSING_LOCK:
            for node_id, node_info in NODES.items():
                if current_time - node_info['last_active'] > 15:  # 15 seg timeout
                    inactive_nodes.append(node_id)

            for node_id in inactive_nodes:
                logger.warning(f"⏰ Timeout for node {node_id}, disconnecting")
                try:
                    if 'conn' in NODES[node_id]:
                        NODES[node_id]['conn'].sendall(b"BAD_TIMEOUT")
                        NODES[node_id]['conn'].close()
                except:
                    pass
                del NODES[node_id]

def start_server():
    """Starts the principal service"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((HOST, PORT))
            server_socket.listen(5)  # Queue of pending connections
            logger.info(f"🟢 Server started on {HOST}:{PORT}")

            # Start auxiliary threads
            threading.Thread(target=process_node_data, daemon=True).start()
            threading.Thread(target=monitor_nodes, daemon=True).start()

            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=handle_node_connection, args=(conn, addr)).start()

    except KeyboardInterrupt:
        logger.info("🛑 Server stopped by user")
    finally:
        sys.exit(0)

if __name__ == "__main__":
    start_server()

# # Diccionario para mantener las sesiones de pares
# sessions = {}
# server_running = True

# ###############################################################
# class SessionManager:
#     def __init__(self):
#         self.sessions = {}
#         self.counter = 1
#         self.lock = threading.Lock()

#     def add_client(self, client_socket):
#         with self.lock:
#             if len(self.sessions) % 2 == 0:
#                 session_id = self.counter
#                 self.sessions[session_id] = [client_socket]
#                 self.counter += 1
#                 return session_id, True  # Nueva sesión
#             else:
#                 session_id = self.counter - 1
#                 self.sessions[session_id].append(client_socket)

#                 self._send_seed_to_session(session_id) # Envío de semilla a las sesiones
#                 return session_id, False  # Sesión completada

#     def _send_seed_to_session(self, session_id):
#         """Envía la semilla a ambos clientes de la sesión"""
#         if session_id in self.sessions and len(self.sessions[session_id]) == 2:
#             # seed_message = f"SEMILLA:{genSeed()}".encode()
#             for client in self.sessions[session_id]:
#                 try:
#                     client.send(seed_message)
#                 except Exception as e:
#                     print(f"Error enviando semilla: {e}")

#     def remove_client(self, session_id, client_socket):
#         with self.lock:
#             if session_id in self.sessions:
#                 if client_socket in self.sessions[session_id]:
#                     self.sessions[session_id].remove(client_socket)
#                     if not self.sessions[session_id]:
#                         del self.sessions[session_id]

# session_manager = SessionManager()
# ###############################################################


# ###############################################################
# # Manejo de los clientes
# def handle_client(client_socket, client_address):
#     global server_running

#     try:
#         print(f"Nueva conexión desde {client_address}")

#         # Asignar el cliente a una sesión
#         session_id, is_new_session = session_manager.add_client(client_socket)

#         if is_new_session:
#             print(f"Esperando al segundo cliente para la sesión {session_id}")
#             client_socket.send("Esperando al segundo cliente...".encode())
#         else:
#             print(f"Sesión {session_id} iniciada con dos clientes")



#             # Asignar aleatoriamente el primer turno
#             first_turn = random.choice([0, 1])
#             session_manager.sessions[session_id][first_turn].send("Sesión iniciada. Es tu turno.".encode())
#             session_manager.sessions[session_id][1 - first_turn].send("Sesión iniciada. Esperando tu turno...".encode())

#         # Manejar la comunicación entre los clientes
#         while server_running:
#             try:
#                 message = client_socket.recv(1024).decode()
#                 if not message:
#                     break

#                 print(f"Mensaje recibido en la sesión {session_id}: {message}")

#                 # Enviar el mensaje al otro cliente en la sesión
#                 if session_id in session_manager.sessions:
#                     other_index = 1 if client_socket == session_manager.sessions[session_id][0] else 0
#                     if other_index < len(session_manager.sessions[session_id]):
#                         session_manager.sessions[session_id][other_index].send(message.encode())

#                     # Manejo de turnos
#                     if message == "P":
#                         session_manager.sessions[session_id][other_index].send("Es tu turno.".encode())

#             except (ConnectionResetError, ConnectionAbortedError):
#                 print(f"Cliente {client_address} desconectado")
#                 break
#             except Exception as e:
#                 print(f"Error en la sesión {session_id}: {str(e)}")
#                 break

#     finally:
#         # Eliminar el cliente de la sesión
#         session_manager.remove_client(session_id, client_socket)
#         client_socket.close()
#         print(f"Conexión con {client_address} cerrada")
# ###############################################################


# ###############################################################
# # Handler para el cierre del servidor y no quede el puerto vivo
# def signal_handler(sig, frame):
#     global server_running
#     print("\nRecibida señal de terminación, cerrando servidor...")
#     server_running = False
    
#     # Forzar cierre del socket principal
#     if 'server_socket' in globals():
#         try:
#             # Crear conexión temporal para desbloquear accept()
#             temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             temp_socket.connect(("127.0.0.1", 12345))
#             temp_socket.close()
#         except:
#             pass
        
#         try:
#             server_socket.close()
#             print("Socket del servidor cerrado correctamente")
#         except Exception as e:
#             print(f"Error cerrando socket del servidor: {str(e)}")
    
#     sys.exit(0)
# ###############################################################


# ###############################################################
# def start_weather_services():
    

    
#     global server_socket, server_running
    
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

#     # Verify if a parameter was passed in the execution of main
#     host =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_IP')
#     port = os.getenv('RECEIVER_PORT')
    

#     # Ejecutar los servicios de flood_sensor.py y rain_gauge.py y quedarse esperando 
#       a alguna respuesta para invocar a sus respectivas funciones que hagan fetch al Receiver !!!
#     try:
#         server_socket.bind((host, port))
#         server_socket.listen(5)
#         print(f"Servidor iniciado en {host}:{port}")
        
#         # Configuración de manejo de señales
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)
        
#         while server_running:
#             try:
#                 client_socket, client_address = server_socket.accept()
#                 client_thread = threading.Thread(
#                     target=handle_client, 
#                     args=(client_socket, client_address),
#                     daemon=True
#                 )
#                 client_thread.start()
#             except OSError as e:
#                 if server_running:
#                     print(f"Error aceptando conexión: {str(e)}")
#                     break
#     finally:
#         if 'server_socket' in globals():
#             server_socket.close()
#         print("Servidor detenido")
# ###############################################################


# if __name__ == "__main__":
#     start_weather_services()
