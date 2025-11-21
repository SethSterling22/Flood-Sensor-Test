"""
This program opens a server through which different
nodes can connect. It also synchronizes the information
these nodes send every minute and queues it for upload.
"""



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
from dotenv import load_dotenv



# === ENVIRONMENT VARIABLES ===
load_dotenv("./Env/.env.config")
HOST = "0.0.0.0" # All transmitters
PORT = int(os.getenv("RECEIVER_PORT") or 4040)


# === GLOBAL VARIABLES AND LOCKS ===
STOP_EVENT = threading.Event()
# Storage {NODE_ID: socket_object}
CLIENTS_INDEX = {} 
CLIENT_SEND_EVENTS = {}
INDEX_LOCK = threading.Lock() # For the clients to start index
CSV_WRITE_QUEUE = queue.Queue() # Thread for the CSV writing while handling other data



# === SAVE FILES PATH ===
LOG_DIR = "./Logs/"
CSV_DIR = os.path.join(LOG_DIR,"Water_data/")
CSV_FILE = os.path.join(CSV_DIR, 'metrics_data.csv')
# Create the directories if not exist
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



# ====== CSV FILE CONFIGURATION ======
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
        logger.error("❌ ERROR setting up CSV: %s", e)



def write_to_csv_sync(data_list, node_id):
    """
    Save the data Chunks in the CSV file
    """

    # Calculate the time of the received information
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # List with RAW data
    rows = []

    # JSON serialization for CSV
    for item in data_list:
        if isinstance(item, dict) or isinstance(item, list):
            # Turn the list to a JSON string to save it as "Raw_Data"
            raw_data_str = json.dumps(item)
        else:
            # Base case
            raw_data_str = str(item)

        # CSV FORMAT: | NODE_ID | TIMESTAMP | JSON SENSOR DATA |
        rows.append([node_id, now, raw_data_str])

    try:
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        logger.info("✅ Saved %d data chunks from NODE ID: %s", len(data_list), node_id)
    except Exception as e:
        logger.error("❌ Error writing on CSV: %s", e)



def csv_writer_job():
    """
    Thread dedicated to consuming tasks from the
    CSV_WRITE_QUEUE and writing them to the file.
    """
    
    logger.info("📝 CSV Writer thread started.")
    while not STOP_EVENT.is_set():
        try:
            # Esperar por datos en la cola con un timeout corto para revisar STOP_EVENT
            # El item_data es una tupla: (data_list, node_id)
            item_data = CSV_WRITE_QUEUE.get(timeout=1)
            
            # Si recibimos un elemento, procesarlo
            data_list, node_id = item_data
            
            # Llamar a la función síncrona de escritura
            write_to_csv_sync(data_list, node_id)
            
            # Mark as done
            CSV_WRITE_QUEUE.task_done()

        except queue.Empty:
            # Si la cola está vacía, el bucle continúa y revisa STOP_EVENT
            continue
        except Exception as e:
            logger.error("❌ Error processing CSV queue task: %s", e)
            # Si hay un error, aún debemos marcar la tarea como hecha para que la cola avance
            if 'item_data' in locals():
                CSV_WRITE_QUEUE.task_done()
                
    logger.info("📝 CSV Writer thread terminated.")
# ====== CSV FILE CONFIGURATION ======



# def process_queue():
#     """Process queued data and manage uploads"""
#     last_upload = time.time()
#     UPLOAD_INTERVAL = 3600  # 1 hour
    
#     while True:
#         # Process at minute boundaries
#         now = time.time()
#         time.sleep(60 - (now % 60))
        
#         # Collect all available data
#         records_to_process = []
#         while not NODE_DATA_QUEUE.empty():
#             try:
#                 item = NODE_DATA_QUEUE.get_nowait()
                
#                 # Create separate records for each metric
#                 for sensor, value in item['metrics'].items():
#                     records_to_process.append({
#                         'timestamp': item['timestamp'],
#                         'node_id': item['node_id'],
#                         'sensor': sensor.replace('🌧️ ', '').replace('💧 ', ''),
#                         'value': value
#                     })
                
#                 NODE_DATA_QUEUE.task_done()
#             except queue.Empty:
#                 break
        
#         # Write to CSV and buffer for upload
#         if records_to_process:
#             write_to_csv(records_to_process)
            
#             with PROCESSING_LOCK:
#                 CSV_BUFFER.extend(records_to_process)
#                 logger.info(f"📊 Buffer: {len(CSV_BUFFER)} records")
                
#                 # Hourly upload check
#                 if time.time() - last_upload >= UPLOAD_INTERVAL and CSV_BUFFER:
#                     try:
#                         uploader_metrics(CSV_BUFFER.copy())
#                         CSV_BUFFER.clear()
#                         last_upload = time.time()
#                     except Exception as e:
#                         logger.error(f"❌ Upload failed: {str(e)}")

# def uploader_metrics(data: List[Dict[str, Any]]):
#     """Simulated upload function"""
#     logger.info(f"⬆️ Uploading {len(data)} records")
#     # Implement actual upload logic here



##########################################################################
def handle_client(conn, addr):
    """
    Manage the connection with an individual client.
    """

    node_id = None
    client_address = f"{addr[0]}:{addr[1]}"
    thread_name = threading.current_thread().name

    logger.info("[%s] 🤝 New connection from: %s", thread_name, client_address)

    try:
        # 1. Confirm connection and wait for ID
        conn.sendall(b"CONNECTED")

        # 2. Receive NODE_ID
        conn.settimeout(60) # Timeout para el registro inicial
        node_id_bytes = conn.recv(1024).strip()

        if not node_id_bytes:
            logger.warning("[%s] Client %s did not send ID. Closing...", thread_name, client_address)
            return

        # Catch node ID
        node_id = node_id_bytes.decode()
        logger.info("[%s] NODE_ID Received: %s", thread_name, node_id)

        # 3. Index the client
        with INDEX_LOCK:
            if node_id in CLIENTS_INDEX:
                logger.warning("[%s] Replacing existing connection for ID: %s", thread_name, node_id)
            CLIENTS_INDEX[node_id] = conn

        conn.sendall(b"ID_RECEIVED")
        logger.info("✅ Sending Response [ID_RECEIVED]")

        # Little wait to prevent the concatenation
        time.sleep(0.05)

        client_event = threading.Event()
        with INDEX_LOCK:
            CLIENT_SEND_EVENTS[node_id] = client_event

        # Check if there is any data pending to send in queue (DRENAJE DE BUFFER)
        try:
            conn.settimeout(0.1) 
            response_drain = conn.recv(1024) 
            if response_drain:
                logger.warning("[%s] Drained leftover data after ID_RECEIVED: %s", thread_name, response_drain.decode().strip())
        except socket.timeout:
            # OK, nothig was pending
            pass
        except Exception as e:
            logger.error("[%s] ❌ Error draining buffer: %s", thread_name, e)
        
        # 4. Principal loop, data receival
        while not STOP_EVENT.is_set():

            if not client_event.wait(timeout=45):
                continue 

            client_event.clear() 

            try:
                # Client will send data after receive READY_TO_INDEX
                conn.settimeout(45) 

                # 5. Process data length (length protocol)
                #################################################
                
                # 1. Read length prefix (8 bytes)
                length_bytes = conn.recv(8)

                if not length_bytes:
                    raise ConnectionResetError("Client disconnected during transfer.")

                # 2. Decode and validate length
                length_str = length_bytes.decode()

                try:
                    data_length = int(length_str)
                except ValueError:
                    logger.error("[%s] ❌ Protocol error: Invalid length field: %s", thread_name, length_str)
                    conn.sendall(b"PROTOCOL_ERROR")
                    return

                data_bytes = b''
                bytes_received = 0

                # 3. Receive the data transfer in chunks
                while bytes_received < data_length:
                    remaining_bytes = data_length - bytes_received
                    # Read chunks, limited by 4096 or left bytes
                    chunk = conn.recv(min(4096, remaining_bytes)) 
                    if not chunk:
                        raise ConnectionResetError("❌ Connection lost during data transfer.")
                    data_bytes += chunk
                    bytes_received += len(chunk)

                # 6. Process and save data (JSON)
                payload = data_bytes.decode()

                # 7. Logic of: NO_DATA or JSON
                if payload == "NO_DATA":
                    logger.info("[%s] Client %s reported NO_DATA.", thread_name, node_id)
                else:
                    try:
                        data_list = json.loads(payload)
                        logger.info("[%s] Received %d chunks of data from %s. Enqueuing for CSV writer.", thread_name, len(data_list), node_id)

                        # Save data with Thread
                        CSV_WRITE_QUEUE.put((data_list, node_id))

                    except json.JSONDecodeError:
                        logger.error(f"[{thread_name}] ❌ Error decoding JSON received from {node_id}.", )
                        conn.sendall(b"JSON_ERROR")
                        continue

                # 8. Send ACK to client
                conn.sendall(b"DATA_RECEIVED")

            except socket.timeout:
                logger.warning(f"[{thread_name}] Client {node_id} doesn't send data on time (Timeout).")
                continue

            except Exception as e:
                logger.error(f"[{thread_name}] ❌ Error en la comunicación con {node_id}: {e}")
                break

    except Exception as e:
        logger.error(f"[{thread_name}] ❌ Error during client handling: {e}")

    finally:
        # Cleanup: Delete the client from index
        if node_id:
            with INDEX_LOCK:

                if node_id in CLIENTS_INDEX:
                    del CLIENTS_INDEX[node_id]
                    logger.info("[%s] Cliente %s desindexado y conexión cerrada.", thread_name, node_id)

                if node_id in CLIENT_SEND_EVENTS:
                    del CLIENT_SEND_EVENTS[node_id]

        if conn:
            conn.close()
##########################################################################



def scheduler_job():
    """
    Thread to send the sync signal to the clients each minte.
    """

    # Initial synchronization of the system clock (similar to client)
    now = datetime.datetime.now()
    seconds_to_wait = 60 - now.second
    logger.info("⏰ Scheduler waiting %s s for initial sync.", seconds_to_wait )
    time.sleep(seconds_to_wait)

    while not STOP_EVENT.is_set():
        now_minute = datetime.datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{now_minute}] 🔔 Sending READY_TO_INDEX signal to {len(CLIENTS_INDEX)} clients...")

        # Iterate a copy to prevent thread modify the list
        with INDEX_LOCK:
            clients_to_check = list(CLIENTS_INDEX.items())

        # Sending the READY_TO_INDEX signal
        for node_id, conn in clients_to_check:
            try:
                if node_id in CLIENT_SEND_EVENTS:
                    CLIENT_SEND_EVENTS[node_id].set() 

                conn.sendall(b"READY_TO_INDEX")
            except Exception as e:
                # If fails, client must be disconnected
                # The thread handle_client will be cleaned
                logger.warning("❌ Error sending on Node: %s:\n %s \n Marked to clean...", node_id, e)
                try:
                    conn.close() 
                except:
                    pass

        # Wait 1 minute for the next cicle
        time.sleep(60)



def main_server():
    """
    Función principal que inicia el servidor y los hilos.
    """

    # Set up the CSV files
    setup_csv()

    # 1. Start the thread for Scheduler
    scheduler = threading.Thread(target=scheduler_job, name="Scheduler")
    scheduler.start()

    # Start data saver thread
    csv_writer = threading.Thread(target=csv_writer_job, name="CSV-Writer")
    csv_writer.start()

    # 2. Starting the socket servidor
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allow to reuse the Port
            s.bind((HOST, PORT))
            s.listen()
            logger.info(f"📡 Server listening to {HOST}:{PORT}")

            # 3. Loop to accept connections
            s.settimeout(1) # Timeout to check STOP_EVENT
            while not STOP_EVENT.is_set():
                try:
                    conn, addr = s.accept()
                    # Start a new thread to handle the client
                    client_thread = threading.Thread(target=handle_client, args=(conn, addr), name=f"Client-{addr[1]}")
                    client_thread.start()

                except socket.timeout:
                    # Timeout to check if STOP_EVENT is being activated
                    continue
                except Exception as e:
                    logger.error(f"❌ Error accepting the connection: {e}")
                    break

    except Exception as e:
        logger.critical(f"❌ Fatal error starting the server: {e}")

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
        # Tiny lapse to detect the stop
        time.sleep(2)





# """
# This program opens a server through which different
# nodes can connect. It also synchronizes the information
# these nodes send every minute and queues it for upload.
# """

# #import threading
# #import random
# #import signal




# import os
# import sys
# import time
# import json
# import queue
# import socket
# import logging
# import threading
# from datetime import datetime, timedelta
# from dotenv import load_dotenv
# from typing import Dict, Any, List
# import csv

# # === ENVIRONMENT VARIABLES ===
# load_dotenv("./Env/.env.config")
# HOST = "0.0.0.0"
# PORT = int(os.getenv("RECEIVER_PORT") or 4040)

# # === GLOBAL VARIABLES ===
# NODES: Dict[str, Any] = {}
# NODE_DATA_QUEUE = queue.Queue()
# CSV_BUFFER: List[Dict[str, Any]] = []
# PROCESSING_LOCK = threading.Lock()

# LOG_DIR = "./Logs/"
# CSV_DIR = "./Water_data/"
# CSV_FILE = os.path.join(CSV_DIR, 'metrics_data.csv')

# os.makedirs(LOG_DIR, exist_ok=True)
# os.makedirs(CSV_DIR, exist_ok=True)

# # === LOGGING SETUP ===
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(os.path.join(LOG_DIR, 'metrics_receiver.log'), encoding='utf-8'),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)


# class NodeConnectionManager:
#     """Manages all node connections and synchronization"""
    
#     def __init__(self):
#         self.minute_events: Dict[str, threading.Event] = {}
#         self.node_timers: Dict[str, datetime] = {}
#         self.lock = threading.Lock()
    
#     def register_node(self, node_id: str):
#         """Register a new node connection"""
#         with self.lock:
#             if node_id not in self.minute_events:
#                 self.minute_events[node_id] = threading.Event()
#                 self.node_timers[node_id] = datetime.now()
#                 logger.info(f"⏱️ Registered timer for node {node_id}")
    
#     def unregister_node(self, node_id: str):
#         """Remove a node connection"""
#         with self.lock:
#             if node_id in self.minute_events:
#                 del self.minute_events[node_id]
#                 del self.node_timers[node_id]
#                 logger.info(f"⏱️ Unregistered timer for node {node_id}")
    
#     def wait_for_minute(self, node_id: str) -> bool:
#         """Wait until the next minute boundary for a node"""
#         with self.lock:
#             if node_id not in self.minute_events:
#                 return False
            
#             event = self.minute_events[node_id]
#             last_time = self.node_timers[node_id]
#             next_minute = (last_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
#             wait_seconds = (next_minute - datetime.now()).total_seconds()
            
#             if wait_seconds > 0:
#                 logger.debug(f"⏱️ Node {node_id} waiting {wait_seconds:.1f}s until next minute")
#                 return event.wait(wait_seconds)
#             return True
    
#     def notify_minute_elapsed(self):
#         """Notify all nodes that a minute has elapsed"""
#         with self.lock:
#             current_time = datetime.now()
#             for node_id, event in self.minute_events.items():
#                 self.node_timers[node_id] = current_time
#                 event.set()
#                 event.clear()
#             logger.debug("⏱️ All nodes notified of minute change")

# node_manager = NodeConnectionManager()

# def handle_node_connection(conn: socket.socket, addr: tuple):
#     """Manage persistent node connection with minute synchronization"""
#     node_id = None
#     try:
#         # Step 1: Initial Handshake
#         conn.settimeout(30)
#         conn.sendall(b"NODE_ID_REQUEST")
#         node_id = conn.recv(1024).decode('utf-8').strip()
        
#         if not node_id.startswith("NODE_"):
#             raise ValueError("Invalid node ID format")
        
#         # Register node with connection manager
#         node_manager.register_node(node_id)
        
#         with PROCESSING_LOCK:
#             NODES[node_id] = {
#                 'conn': conn,
#                 'addr': addr,
#                 'last_active': time.time(),
#                 'status': 'connected'
#             }
        
#         logger.info(f"🆔 Node {node_id} connected from {addr}")
#         conn.sendall(b"READY")
#         conn.settimeout(None)  # Reset to blocking mode

#         # Step 2: Minute-synchronized data collection
#         while True:
#             # Wait for next minute boundary
#             if not node_manager.wait_for_minute(node_id):
#                 break
            
#             try:
#                 # Send READY signal at minute boundary
#                 conn.sendall(b"READY")
                
#                 # Wait for data (with 65s timeout in case of issues)
#                 conn.settimeout(65)
#                 data = conn.recv(4096)
#                 conn.settimeout(None)
                
#                 if not data:
#                     break  # Connection closed
                
#                 current_time = time.time()
#                 message = json.loads(data.decode('utf-8'))
#                 logger.info(f"📥 Received data from {node_id}")

#                 # Validate message
#                 if not all(key in message for key in ['node_id', 'metrics', 'timestamp']):
#                     raise ValueError("Invalid message format")
                
#                 # Update activity
#                 with PROCESSING_LOCK:
#                     NODES[node_id]['last_active'] = current_time

#                 # Queue for processing
#                 NODE_DATA_QUEUE.put({
#                     'node_id': node_id,
#                     'timestamp': message['timestamp'],
#                     'metrics': message['metrics']
#                 })
                
#                 conn.sendall(b"OK_QUEUED")

#             except socket.timeout:
#                 logger.warning(f"⏰ Node {node_id} missed data window")
#                 continue
#             except (json.JSONDecodeError, ValueError) as e:
#                 logger.error(f"❌ Invalid data from {node_id}: {str(e)}")
#                 conn.sendall(b"BAD_FORMAT")
#             except Exception as e:
#                 logger.error(f"🔴 Error with {node_id}: {str(e)}")
#                 raise

#     except (ConnectionResetError, BrokenPipeError):
#         logger.warning(f"⚠️ Connection reset by node {node_id or addr}")
#     except socket.timeout:
#         logger.warning(f"⏰ Handshake timeout for node {node_id or addr}")
#     except Exception as e:
#         logger.error(f"🔴 Connection error with {node_id or addr}: {str(e)}")
#     finally:
#         if node_id:
#             with PROCESSING_LOCK:
#                 if node_id in NODES:
#                     del NODES[node_id]
#             node_manager.unregister_node(node_id)
#         conn.close()
#         logger.info(f"🚪 Node {node_id or addr} disconnected")

# def minute_timer():
#     """Global timer that synchronizes all nodes to minute boundaries"""
#     while True:
#         now = datetime.now()
#         sleep_time = 60 - now.second
#         time.sleep(sleep_time)
#         node_manager.notify_minute_elapsed()
#         logger.debug("⏰ Global minute synchronization")

# def write_to_csv(data_list: List[Dict[str, Any]]):
#     """Write collected data to CSV"""
#     if not data_list:
#         return

#     fieldnames = ['timestamp', 'node_id', 'sensor', 'value']
#     file_exists = os.path.exists(CSV_FILE)
    
#     try:
#         with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#             if not file_exists:
#                 writer.writeheader()
            
#             for record in data_list:
#                 writer.writerow(record)
        
#         logger.info(f"💾 Wrote {len(data_list)} records to CSV")

#     except Exception as e:
#         logger.error(f"❌ CSV write error: {str(e)}")

# def process_queue():
#     """Process queued data and manage uploads"""
#     last_upload = time.time()
#     UPLOAD_INTERVAL = 3600  # 1 hour
    
#     while True:
#         # Process at minute boundaries
#         now = time.time()
#         time.sleep(60 - (now % 60))
        
#         # Collect all available data
#         records_to_process = []
#         while not NODE_DATA_QUEUE.empty():
#             try:
#                 item = NODE_DATA_QUEUE.get_nowait()
                
#                 # Create separate records for each metric
#                 for sensor, value in item['metrics'].items():
#                     records_to_process.append({
#                         'timestamp': item['timestamp'],
#                         'node_id': item['node_id'],
#                         'sensor': sensor.replace('🌧️ ', '').replace('💧 ', ''),
#                         'value': value
#                     })
                
#                 NODE_DATA_QUEUE.task_done()
#             except queue.Empty:
#                 break
        
#         # Write to CSV and buffer for upload
#         if records_to_process:
#             write_to_csv(records_to_process)
            
#             with PROCESSING_LOCK:
#                 CSV_BUFFER.extend(records_to_process)
#                 logger.info(f"📊 Buffer: {len(CSV_BUFFER)} records")
                
#                 # Hourly upload check
#                 if time.time() - last_upload >= UPLOAD_INTERVAL and CSV_BUFFER:
#                     try:
#                         uploader_metrics(CSV_BUFFER.copy())
#                         CSV_BUFFER.clear()
#                         last_upload = time.time()
#                     except Exception as e:
#                         logger.error(f"❌ Upload failed: {str(e)}")

# def uploader_metrics(data: List[Dict[str, Any]]):
#     """Simulated upload function"""
#     logger.info(f"⬆️ Uploading {len(data)} records")
#     # Implement actual upload logic here

# def monitor_nodes():
#     """Monitor node connections and handle timeouts"""
#     while True:
#         time.sleep(5)
#         current_time = time.time()
#         inactive_nodes = []

#         with PROCESSING_LOCK:
#             for node_id, node_info in list(NODES.items()):
#                 if current_time - node_info['last_active'] > 120:  # 2 minute timeout
#                     inactive_nodes.append(node_id)

#             for node_id in inactive_nodes:
#                 logger.warning(f"⏰ Timeout for node {node_id}")
#                 try:
#                     if 'conn' in NODES[node_id]:
#                         NODES[node_id]['conn'].sendall(b"BAD_TIMEOUT")
#                         NODES[node_id]['conn'].close()
#                 except:
#                     pass
#                 del NODES[node_id]
#                 node_manager.unregister_node(node_id)

# def start_server():
#     """Start the server and worker threads"""
#     try:
#         # Start worker threads
#         threading.Thread(target=minute_timer, daemon=True).start()
#         threading.Thread(target=process_queue, daemon=True).start()
#         threading.Thread(target=monitor_nodes, daemon=True).start()

#         # Main server socket
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#             server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             server_socket.bind((HOST, PORT))
#             server_socket.listen(10)
#             logger.info(f"🟢 Server started on {HOST}:{PORT}")

#             while True:
#                 conn, addr = server_socket.accept()
#                 threading.Thread(target=handle_node_connection, args=(conn, addr)).start()

#     except KeyboardInterrupt:
#         logger.info("🛑 Server stopped by user")
#     except Exception as e:
#         logger.error(f"🔴 Server error: {str(e)}")
#     finally:
#         sys.exit(0)

# if __name__ == "__main__":
#     start_server()


############################################################################################################


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



# def handle_node_connection(conn, addr):
#     """
#     Manage each NODE individually and send
#     send its data to process and upload
#     """

#     node_id = None
#     try:
#         # Step 1: Node identification
#         conn.sendall(b"NODE_ID_REQUEST")
#         # conn.sendall(b"READY")
#         node_id_response = conn.recv(1024).decode('utf-8').strip()
        
#         if not node_id_response:
#             raise ValueError("Empty node ID received")
        
#         node_id = node_id_response
#         with PROCESSING_LOCK:
#             NODES[node_id] = {
#                 'conn': conn,
#                 'addr': addr,
#                 'last_active': time.time(),
#                 'status': 'connected'
#             }
        
#         logger.info(f"🆔 Node {node_id} connected from {addr}")
#         conn.sendall(b"READY")
#         #conn.sendall(b"OK_AUTH") 

#         # Step 2: Data retreival 
#         while True:
#             data = conn.recv(4096)
#             if not data:
#                 logger.warning(f"📥 Not received any data from {node_id}: {message['thread']}")
#                 break

#             current_time = time.time()
#             try:
#                 message = json.loads(data.decode('utf-8'))
#                 logger.info(f"📥 Received data from {node_id}: {message['thread']}")

#                 # Basic message validation
#                 if 'thread' not in message or 'data' not in message:
#                     raise ValueError("Invalid message format")

#                 # Update current activity
#                 with PROCESSING_LOCK:
#                     NODES[node_id]['last_active'] = current_time

#                 #################################################################
#                 # Add to the processing queue
#                 NODE_QUEUE.put({
#                     'node_id': node_id,
#                     'message': message,
#                     'timestamp': current_time
#                 })
#                 #################################################################

#                 # Response
#                 conn.sendall(b"OK_QUEUED")

#             except (json.JSONDecodeError, ValueError) as e:
#                 logger.error(f"❌ Error processing data from {node_id}: {str(e)}")
#                 conn.sendall(b"BAD_FORMAT")

#     except ConnectionResetError:
#         logger.warning(f"⚠️ Connection reset by node {node_id or addr}")
#     except Exception as e:
#         logger.error(f"🔴 Error with node {node_id or addr}: {str(e)}")
#     finally:
#         if node_id:
#             with PROCESSING_LOCK:
#                 if node_id in NODES:
#                     del NODES[node_id]
#         conn.close()
#         logger.info(f"🚪 Node {node_id or addr} disconnected")


# def process_node_data():
#     """
#     Procesa los datos de los nodos en orden de llegada
#     """
    
#     while True:
#         try:
#             # Obtener el siguiente elemento de la cola (bloqueante)
#             item = NODE_QUEUE.get()
#             node_id = item['node_id']
#             message = item['message']
#             timestamp = item['timestamp']

#             #################################################################
#             # PROCESAMIENTO!!! ----> Send it to Upload
#             # Simular procesamiento (aquí iría tu lógica real)
#             logger.info(f"🔧 Processing {message['thread']} from {node_id}")
#             logger.info(f"Data: {message['data']}")
#             time.sleep(1)  # Simula tiempo de procesamiento
#             #################################################################
            
#             # Mark as completed
#             NODE_QUEUE.task_done()
#             logger.info(f"✅ Processed {message['thread']} from {node_id}")

#         except Exception as e:
#             logger.error(f"❌ Error processing data: {str(e)}")

# def monitor_nodes():
#     """
#     Manage the interactive NODES and reject the ones with timeout
#     """

#     while True:
#         time.sleep(5)  # Check out each 5 seconds
#         current_time = time.time()
#         inactive_nodes = []

#         with PROCESSING_LOCK:
#             for node_id, node_info in NODES.items():
#                 if current_time - node_info['last_active'] > 90:  # 15 seg timeout
#                     inactive_nodes.append(node_id)

#             for node_id in inactive_nodes:
#                 logger.warning(f"⏰ Timeout for node {node_id}, disconnecting")
#                 try:
#                     if 'conn' in NODES[node_id]:
#                         NODES[node_id]['conn'].sendall(b"BAD_TIMEOUT")
#                         NODES[node_id]['conn'].close()
#                 except:
#                     pass
#                 del NODES[node_id]

# def start_server():
#     """Starts the principal service"""
#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#             server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             server_socket.bind((HOST, PORT))
#             server_socket.listen(5)  # Queue of pending connections
#             logger.info(f"🟢 Server started on {HOST}:{PORT}")

#             # Start auxiliary threads
#             threading.Thread(target=process_node_data, daemon=True).start()
#             threading.Thread(target=monitor_nodes, daemon=True).start()

#             while True:
#                 conn, addr = server_socket.accept()
#                 threading.Thread(target=handle_node_connection, args=(conn, addr)).start()

#     except KeyboardInterrupt:
#         logger.info("🛑 Server stopped by user")
#     finally:
#         sys.exit(0)

# if __name__ == "__main__":
#     start_server()



