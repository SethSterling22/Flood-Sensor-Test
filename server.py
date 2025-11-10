"""
This program opens a server through which different
nodes can connect. It also synchronizes the information
these nodes send every minute and queues it for upload.
"""

#import threading
#import random
import signal



import os
import sys
import csv
import time
import json
import queue
import socket
import logging
import datetime
import threading
from typing import Dict, Any, List
from dotenv import load_dotenv



# === ENVIRONMENT VARIABLES ===
load_dotenv("./Env/.env.config")
HOST = "0.0.0.0" # All transmitters
PORT = int(os.getenv("RECEIVER_PORT") or 4040)

# === GLOBAL VARIABLES ===
# NODES: Dict[str, Any] = {}
NODE_DATA_QUEUE = queue.Queue()
CSV_BUFFER: List[Dict[str, Any]] = []
# PROCESSING_LOCK = threading.Lock()

LOG_DIR = "./Logs/"
CSV_DIR = os.path.join(LOG_DIR,"Water_data/")
CSV_FILE = os.path.join(CSV_DIR, 'metrics_data.csv')

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'metrics_receiver.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# --- Configuración ---


# --- Variables Globales y Locks ---
STOP_EVENT = threading.Event()
# Almacena {NODE_ID: socket_object}
CLIENTS_INDEX = {} 
CLIENT_SEND_EVENTS = {}
INDEX_LOCK = threading.Lock() # Para asegurar acceso seguro a CLIENTS_INDEX

# # Configuración básica del logger
# logging.basicConfig(level=logging.INFO, 
#                     format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# --- Configuración de Archivo CSV ---
def setup_csv():
    """
    Create the CSV and add the header.
    """
    try:
        with open(CSV_FILE, mode='w', newline='') as file:
            writer = csv.writer(file)
            # write on the CSV the data
            writer.writerow(['Node_ID', 'Timestamp', 'Raw_Data']) 
        logger.info("💾 File data %s ready with headers.", CSV_FILE)
    except Exception as e:
        logger.error("ERROR setting up CSV: %s", e)


def save_data_to_csv(data_list, node_id):
    """
    Guarda una lista de puntos de datos en el archivo CSV.
    """

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Asumimos que data_list es una lista de strings (puntos de datos crudos)
    rows = []
    for raw_data in data_list:
        rows.append([now, node_id, raw_data])
        
    try:
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        logger.info("✅ Guardados %d puntos de datos de NODE ID: %s", len(data_list), node_id)
    except Exception as e:
        logger.error("Error al escribir en CSV: %s", e)



def handle_client(conn, addr):
    """
    Maneja la conexión con un cliente individual.
    """

    node_id = None
    client_address = f"{addr[0]}:{addr[1]}"
    thread_name = threading.current_thread().name

    logger.info(f"[{thread_name}] 🤝 Nueva conexión desde {client_address}")

    try:
        # 1. Confirmar conexión y esperar ID
        conn.sendall(b"CONNECTED")

        # 2. Recibir NODE_ID
        conn.settimeout(10) # Timeout para el registro inicial
        node_id_bytes = conn.recv(1024).strip()

        if not node_id_bytes:
            logger.warning("[%s] Cliente %s no envió ID. Cerrando.", thread_name, client_address)
            return

        node_id = node_id_bytes.decode()
        logger.info("[%s] NODE_ID Received: %s", thread_name, node_id)

        # 3. Indexar el cliente
        with INDEX_LOCK:
            if node_id in CLIENTS_INDEX:
                logger.warning(f"[{thread_name}] Reemplazando conexión existente para ID: {node_id}")
            CLIENTS_INDEX[node_id] = conn

        conn.sendall(b"ID_RECEIVED")
        logger.info("Sending Response [ID_RECEIVED]")



        client_event = threading.Event()
        with INDEX_LOCK:
            CLIENT_SEND_EVENTS[node_id] = client_event

        # 4. Bucle principal de recepción de datos
        while not STOP_EVENT.is_set():
            # Aumentar el timeout para esperar datos después de la señal (30 segundos)
            # conn.settimeout(180)

            if not client_event.wait(timeout=30):
            # Si el timeout ocurre, simplemente volvemos a esperar el evento
            # Esto evita que el recv() se bloquee innecesariamente.
                continue 
            
            # El evento se activó (el scheduler envió READY_TO_INDEX)
            client_event.clear() 

            # Recibir datos del cliente
            # La señal READY_TO_INDEX la envía otro hilo (scheduler), no este.
            try:
                # El cliente enviará datos después de recibir READY_TO_INDEX
                conn.settimeout(10) 
                # data_bytes = conn.recv(4096).strip() 
                # conn.settimeout(300) 


                #################################################3

                length_bytes = conn.recv(8)
                    
                if not length_bytes:
                    # Si no se recibió la longitud, el cliente se desconectó
                    raise ConnectionResetError("Cliente se desconectó durante la transferencia.")
                    
                data_length = int(length_bytes.decode())
                
                # 🌟 PASO 2: Leer el payload completo usando un loop
                # Esto garantiza que recibamos todos los bytes, incluso si se envían en chunks
                
                data_bytes = b''
                bytes_received = 0
                
                while bytes_received < data_length:
                    remaining_bytes = data_length - bytes_received
                    chunk = conn.recv(min(4096, remaining_bytes))
                    if not chunk:
                        raise ConnectionResetError("Conexión perdida durante la transferencia de datos.")
                    data_bytes += chunk
                    bytes_received += len(chunk)
                
                conn.settimeout(300) # Volver al timeout de inactividad
                
                # Ya tienes data_bytes, ahora puedes decodificar y procesar
                # 6. Procesar y guardar la data (JSON)
                payload = data_bytes.decode()
                ##################################################

                if not data_bytes:
                    # Conexión cerrada por el cliente
                    logger.warning(f"[{thread_name}] Conexión con ID {node_id} cerrada por el cliente.")
                    break

                
                if payload == "NO_DATA":
                    logger.info(f"[{thread_name}] Cliente {node_id} reportó NO_DATA.")
                    conn.sendall(b"DATA_RECEIVED")
                    continue

                # Procesar la data
                data_list = payload.split('\n')
                logger.info(f"[{thread_name}] Recibidos {len(data_list)} puntos de data de {node_id}.")

                # Guardar data
                save_data_to_csv(data_list, node_id)

                # 7. Semd ACK to client
                conn.sendall(b"DATA_RECEIVED")
                
            except socket.timeout:
                logger.warning(f"[{thread_name}] Cliente {node_id} no envió data a tiempo (Timeout).")
                # Continuamos el bucle, esperamos la próxima señal
                continue


            except Exception as e:
                logger.error(f"[{thread_name}] Error en la comunicación con {node_id}: {e}")
                break

    except Exception as e:
        logger.error(f"[{thread_name}] Error durante el manejo del cliente: {e}")

    finally:
        # Cleanup: Eliminar el cliente del índice
        if node_id:
            with INDEX_LOCK:
                # if node_id in CLIENTS_INDEX:
                #     del CLIENTS_INDEX[node_id]
                #     logger.info("[%s] Cliente %s desindexado y conexión cerrada.", thread_name, node_id)
                if node_id in CLIENT_SEND_EVENTS:
                    del CLIENT_SEND_EVENTS[node_id]
                    logger.info("[%s] Cliente %s desindexado y conexión cerrada.", thread_name, node_id)
                    
        if conn:
            conn.close()



# --------------------------------------------------------------------------

def scheduler_job():
    """
    Hilo para enviar la señal de sincronización a todos los clientes cada minuto.
    """

    # Sincronización inicial con el reloj del sistema (similar al cliente)
    ahora = datetime.datetime.now()
    segundos_a_esperar = 60 - ahora.second
    logger.info(f"⏰ Scheduler esperando {segundos_a_esperar}s para la sincronización inicial.")
    time.sleep(segundos_a_esperar)

    while not STOP_EVENT.is_set():
        now_minute = datetime.datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{now_minute}] 🔔 Enviando señal READY_TO_INDEX a {len(CLIENTS_INDEX)} clientes...")

        # Iterar sobre una copia de las claves para evitar problemas si otro hilo modifica el dict
        with INDEX_LOCK:
            clients_to_check = list(CLIENTS_INDEX.items())

        for node_id, conn in clients_to_check:
            try:
                # Usamos sendall para garantizar el envío
                if node_id in CLIENT_SEND_EVENTS:
                    CLIENT_SEND_EVENTS[node_id].set() 
                
                conn.sendall(b"READY_TO_INDEX")
            except Exception as e:
                # Si falla, el cliente probablemente se desconectó. 
                # El hilo handle_client lo limpiará al intentar recibir datos.
                logger.warning(f"❌ Falló el envío a {node_id}: {e}")

        # Esperar exactamente 60 segundos para el próximo ciclo
        time.sleep(60)

# --------------------------------------------------------------------------

def main_server():
    """
    Función principal que inicia el servidor y los hilos.
    """

    setup_csv() # Prepara el archivo de datos

    # 1. Iniciar el hilo del scheduler (Programador)
    scheduler = threading.Thread(target=scheduler_job, name="Scheduler")
    scheduler.start()

    # 2. Iniciar el socket del servidor
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reusar el puerto
            s.bind((HOST, PORT))
            s.listen()
            logger.info(f"🌍 Servidor escuchando en {HOST}:{PORT}")

            # 3. Bucle de aceptación de conexiones
            s.settimeout(1) # Timeout para revisar STOP_EVENT
            while not STOP_EVENT.is_set():
                try:
                    conn, addr = s.accept()
                    # Iniciar un nuevo hilo para manejar al cliente
                    client_thread = threading.Thread(target=handle_client, args=(conn, addr), name=f"Client-{addr[1]}")
                    client_thread.start()
                except socket.timeout:
                    # El timeout es normal, permite revisar si se ha activado STOP_EVENT
                    continue
                except Exception as e:
                    logger.error(f"Error al aceptar conexión: {e}")
                    break

    except Exception as e:
        logger.critical(f"❌ Error fatal al iniciar el servidor: {e}")

    finally:
        logger.info("🛑 Scheduler stopped, waiting to ending...")
        STOP_EVENT.set()
        scheduler.join()

        # Close all active connection
        with INDEX_LOCK:
            for node_id, conn in CLIENTS_INDEX.items():
                try:
                    conn.close()
                except:
                    pass
            CLIENTS_INDEX.clear()

        logger.info("👋 Server stopped.")
        sys.exit(0)

if __name__ == "__main__":
    try:
        main_server()
    except KeyboardInterrupt:
        logger.info("👋 Stopped by user. Starting secure stop...")
        STOP_EVENT.set()
        # Puedes añadir un pequeño retraso para que el hilo principal detecte el evento
        time.sleep(2)


"""
This program opens a server through which different
nodes can connect. It also synchronizes the information
these nodes send every minute and queues it for upload.
"""

#import threading
#import random
#import signal


#############################################################





# Diccionario para mantener las sesiones de pares
# sessions = {}
# session_counter = 1
# server_running = True


###############################################################
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
#             seed_message = f"SEMILLA:{genSeed()}".encode()
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

# def genSeed():
#     return random.randint(10, 99)

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
# def start_server():
#     global server_socket, server_running
    

#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#     server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    
    

    
#     try:
#         # Open server on HOST and PORT
#         server_socket.bind((HOST, PORT))
#         server_socket.listen(5)
#         logger.info(f"🟢 Server started on {HOST}:{PORT}")
        
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


# #             # Start worker threads
# #             threading.Thread(target=process_node_data, daemon=True).start()
# #             threading.Thread(target=monitor_nodes, daemon=True).start()

# #             while True:
# #                 conn, addr = server_socket.accept()
# #                 threading.Thread(target=handle_node_connection, args=(conn, addr)).start()

# #     except KeyboardInterrupt:
# #         logger.info("🛑 Server stopped by user")
# #     except Exception as e:
# #         logger.error(f"🔴 Server error: {str(e)}")
# #     finally:
# #         sys.exit(0)

# if __name__ == "__main__":
#     start_server()
# ###############################################################




# def imprimir_hola_cada_minuto():
#     """
#     Imprime 'Hola' cada 60 segundos, mostrando la hora actual.
#     """
#     print("🚀 Iniciando programa. El primer 'Hola' se imprimirá en el próximo minuto.")
#     print("--- Presiona Ctrl+C para detener ---")

#     while True:
#         # 1. Obtener la hora actual
#         ahora = datetime.datetime.now()

#         # 2. Calcular cuántos segundos han pasado en este minuto
#         segundos_actuales = ahora.second
        
#         # 3. Calcular el tiempo de espera hasta el inicio del próximo minuto (segundo 00)
#         # Por ejemplo: si son las 10:30:15, quedan 60 - 15 = 45 segundos para el próximo minuto.
#         segundos_a_esperar = 60 - segundos_actuales
        
#         # 4. Esperar el tiempo calculado
#         print(f"Esperando {segundos_a_esperar} segundos para el inicio del próximo minuto...")
#         time.sleep(segundos_a_esperar)

#         # 5. Obtener la hora exacta al inicio del minuto e imprimir
#         ahora_minuto_exacto = datetime.datetime.now()
        
#         # Formato de hora: HH:MM:SS
#         hora_formateada = ahora_minuto_exacto.strftime("%H:%M:%S")

#         print(f"\n[{hora_formateada}] -> ¡Hola! Ya pasó un minuto.")

#         # 6. Esperar exactamente 60 segundos para el siguiente ciclo
#         time.sleep(60)

# if __name__ == "__main__":
#     try:
#         start_server()
#     except KeyboardInterrupt:
#         print("\n🔴 Program stopped by user. 👋")

