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



############################################################################################################
# ====== ENVIRONMENT VARIABLES ======
load_dotenv("./Env/.env.config")
HOST = "0.0.0.0" # All transmitters
PORT = int(os.getenv("RECEIVER_PORT") or 4040)


# ====== GLOBAL VARIABLES AND LOCKS ======
STOP_EVENT = threading.Event()
CLIENTS_INDEX = {} 
CLIENT_SEND_EVENTS = {}
INDEX_LOCK = threading.Lock() # For the clients to start index
CSV_WRITE_QUEUE = queue.Queue() # Thread for the CSV writing while handling other data
CLIENT_SEND_READY_FLAGS = {} 
CLIENT_FLAG_LOCK = threading.Lock()


# ====== SAVE FILES PATH ======
LOG_DIR = "./Logs/"
CSV_DIR = os.path.join(LOG_DIR,"Water_data/")
# Create the directories if not exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
# Provides the filename 
initial_filename = get_next_hourly_filename() 
CSV_FILE = os.path.join(CSV_DIR, initial_filename)


# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'metrics_receiver.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
############################################################################################################



############################################################################################################
# ====== SETUP CSV ======
def setup_csv(filename):
    """
    Create the CSV with the given name and add the header
    """
    try:
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='') as file:
                # write on the CSV the data
                # CSV FORMAT: | NODE_ID | TIMESTAMP | JSON SENSOR DATA |
                csv.writer(file).writerow(['Node_ID', 'Timestamp', 'Raw_Data'])
            logger.info("💾 File data %s ready with headers.", filename)
            return True
        else:
            logger.info("💾 File data %s already exists.", filename)
            return True
    except Exception as e:
        logger.error("❌ ERROR setting up CSV: %s", e)
        return False


# ====== CSV WRITER ======
def csv_writer_job():
    """
    Thread dedicated to consuming tasks from the
    CSV_WRITE_QUEUE and writing them to the file.
    """

    last_upload = time.time()
    UPLOAD_INTERVAL = 3600 # 1 hour
    global CSV_FILE

    logger.info("📝 CSV Writer thread started.")
    while not STOP_EVENT.is_set():
        try:
            item = CSV_WRITE_QUEUE.get(timeout=1)
            data_list, node_id = item

            # Write sync
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows = [[node_id, now, json.dumps(x) if isinstance(x, (dict, list)) else str(x)] for x in data_list]

            try:
                # Write in file
                # CSV FORMAT: | NODE_ID | TIMESTAMP | JSON SENSOR DATA |
                with open(CSV_FILE, mode='a', newline='') as file:
                    csv.writer(file).writerows(rows)

                logger.info("💾 Saved %d items from %s", len(data_list), node_id)
                CSV_WRITE_QUEUE.task_done()

            # UPLOAD SECTION
            except queue.Empty:
                # Check the upload when BUFFER is empty
                if time.time() - last_upload >= UPLOAD_INTERVAL:
                    try:
                        # Call to uploader
                        uploader_metrics()
                        logger.info("⬆️ Upload completed successfully")
                        last_upload = time.time()

                        # Provides the filename
                        new_filename = get_next_hourly_filename()

                        # Create new file and upload the global
                        if setup_csv(new_filename):
                            # Write will point to new file
                            CSV_FILE = new_filename
                            logger.info("🔄 File rotated. New data will be written to: %s", CSV_FILE)
                        else:
                            logger.error("❌ Failed to create new CSV file. Keeping the old file name.")
                    except Exception as e:
                        logger.error(f"❌ Upload failed: {str(e)}")
                continue # GO back to the loop

            except OSError as e:
                # Catch specific OS-level errors (like Errno 9)
                logger.error(f"❌ OS/File Error [{e.errno}]: {e.strerror}. Data RE-QUEUED for safety.")
                # Put the item back in the queue to attempt writing later
                CSV_WRITE_QUEUE.put(item)
                if not STOP_EVENT.wait(10): 
                    continue
                else:
                    break

            except Exception as e:
                logger.error("❌ General I/O Error: %s", e)
                # Failure, discard item after logging
                CSV_WRITE_QUEUE.task_done()

        except queue.Empty:
            continue

        except Exception as e:
            logger.error("❌ Error processing CSV queue task: %s", e)

            # Clean and continue as default
            if 'item_data' in locals():
                CSV_WRITE_QUEUE.task_done()

    logger.info("📝 CSV Writer thread terminated.")


def get_next_hourly_filename():
    """
    Generates the name of file with next hour (H:00:00)
    """
    now = datetime.datetime.now()
    next_hour = now + datetime.timedelta(hours=1)
    return next_hour.strftime("data_%Y%m%d_%H0000.csv")




def uploader_metrics(data: List[Dict[str, Any]]):
    """Simulated upload function"""
    logger.info(f"⬆️ Uploading {len(data)} records")
    # Implement actual upload logic here

############################################################################################################



############################################################################################################
def handle_client(conn, addr):
    """
    Manage the connection with an individual client, with minute-precision synchronization.
    """
    node_id = None
    client_address = f"{addr[0]}:{addr[1]}"
    thread_name = threading.current_thread().name

    logger.info("[%s] 🤝 New connection from: %s", thread_name, client_address)

    try:
        # 1. Confirm connection and wait for ID
        conn.settimeout(10)
        try:
            conn.sendall(b"CONNECTED")
        except Exception:
            conn.close() 
            return

        # 2. Receive NODE_ID
        try:
            node_id_bytes = conn.recv(1024)
            

            if not node_id_bytes:
                logger.warning("[%s] Client %s closed connection or sent no data.", thread_name, client_address)
                conn.close()
                return # Clean Exit
            
            node_id = node_id_bytes.strip().decode()
            
            if not node_id:
                logger.warning("[%s] Client %s sent empty NODE_ID. Closing...", thread_name, client_address)
                conn.close()
                return# Clean Exit
        except socket.timeout:
            logger.warning("[%s] Client %s did not send ID. Closing...", thread_name, client_address)
            conn.close()
            return

        # Catch node ID and concatenate ip and port to be unique
        node_id = f"{node_id}-{addr[1]}"
        logger.info("[%s] NODE_ID Received: %s", thread_name, node_id)

        # Send ACK
        conn.sendall(b"ID_RECEIVED")
        logger.info("✅ Sending Response [ID_RECEIVED] to %s", node_id)

        try:
            # 3. Index the client
            with INDEX_LOCK:
                if node_id in CLIENTS_INDEX:
                    logger.warning("[%s] Replacing existing connection for ID: %s", thread_name, node_id)
                CLIENTS_INDEX[node_id] = conn

        except Exception as e:
            logger.error("❌ Failed to index NODE_ID %s: %s", node_id, e)
            conn.sendall(b"INDEX_FAILED")
            conn.close()
            return


        # ############ Main loop with minute-precision synchronization ############
        while not STOP_EVENT.is_set():

            # Initial synchronization to the next minute boundary
            
            now = datetime.datetime.now()
            sleep_time = 60 - now.second - (now.microsecond / 1_000_000.0)
            
            # Ensure we don't sleep 0 or negative if calculation is tight, 
            # but usually we want to wait for the next "top of the minute"
            if sleep_time < 5.0: 
                sleep_time += 60

            logger.info("[%s] ⏳ Waiting %.2f seconds to trigger %s", thread_name, sleep_time, node_id)
            # time.sleep(sleep_time)
            if STOP_EVENT.wait(sleep_time):
                # Exit inmediatly if STOP_EVENT
                break

            try:
                # Send READY_TO_INDEX exactly at minute boundary
                conn.settimeout(15)
                conn.sendall(b"READY_TO_INDEX")
                logger.info("[%s] 🔔 Sent READY_TO_INDEX to %s at %s", thread_name, node_id, datetime.datetime.now().strftime("%H:%M:%S"))

                # Server will receive data after send READY_TO_INDEX
                conn.settimeout(75) 

                # Process data length (length protocol)
                length_bytes = conn.recv(8)
                if not length_bytes:
                    raise ConnectionResetError("Client disconnected during transfer.")

                length_str = length_bytes.decode()
                try:
                    data_length = int(length_str)
                except ValueError:
                    logger.error("[%s] ❌ Protocol error: Invalid length field: %s", thread_name, length_str)
                    conn.sendall(b"PROTOCOL_ERROR")
                    return

                # Send ACK to client
                try:
                    conn.sendall(b"DATA_RECEIVED")
                    logger.info("👍 DATA_RECEIVED sent to client %s", node_id)
                except:
                    pass

                # Receive the data in chunks
                data_bytes = b''
                bytes_received = 0

                while bytes_received < data_length:
                    remaining_bytes = data_length - bytes_received
                    chunk = conn.recv(min(4096, remaining_bytes)) 
                    if not chunk:
                        raise ConnectionResetError("❌ Connection lost during data transfer.")
                    data_bytes += chunk
                    bytes_received += len(chunk)

                # Process and save data
                try:
                    payload = data_bytes.decode()
                    if payload == "NO_DATA":
                        logger.info("[%s] Client %s reported NO_DATA.", thread_name, node_id)
                    else:
                        try:
                            data_list = json.loads(payload)
                            CSV_WRITE_QUEUE.put((data_list, node_id)) 
                            logger.info("[%s] 📥 Received %d chunks from %s. Enqueuing.", thread_name, len(data_list), node_id)
                        except json.JSONDecodeError:
                            logger.error("[%s] ❌ JSON Error from %s. Data discarded.", thread_name, node_id)
                except Exception as e:
                    logger.error("[%s] ❌ Processing error: %s", thread_name, e)

            except socket.timeout:
                logger.warning("[%s] Client %s doesn't respond on time (Timeout).", thread_name, node_id)
                break

            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                logger.warning("[%s] Client failed data reception: %s. Cleaning up: \n %s", 
                            thread_name, node_id, e)
                break

    except Exception as e:
        logger.error("[%s] ❌ Error during client handling: %s", thread_name, e)

    finally:
        # Cleanup
        if node_id:
            safe_cleanup(node_id, conn)
        elif conn:
            try:
                conn.close()
            except:
                pass


def safe_cleanup(node_id, client_conn=None):
    """
    Desindexa y cierra de forma segura los recursos asociados a un nodo.
    Se utiliza el lock global para asegurar la atomicidad de la operación.
    """
    thread_name = threading.current_thread().name

    with INDEX_LOCK:
        conn_to_close = None

        # 1. Limpiar CLIENTS_INDEX
        if node_id in CLIENTS_INDEX:
            # Esto evita que un hilo antiguo mate una conexión nueva.
            if client_conn is not None and CLIENTS_INDEX[node_id] is not client_conn:
                # El cliente indexado es diferente al que estamos intentando limpiar (es una reconexión).
                # Solo cerramos la conexión que se nos pasó, pero NO la desindexamos.
                logging.warning("[%s] ⚠️ Ignoring desindexing for %s: A newer connection is already active.", thread_name, node_id)
                conn_to_close = client_conn

            else:
                # El nodo no se ha reconectado o somos el dueño de la conexión activa.
                conn_to_close = CLIENTS_INDEX.pop(node_id, None)
                logging.info("[%s] Client %s desindexed.", thread_name, node_id)

        # 2. Limpiar CLIENT_SEND_EVENTS (independiente de la conexión)
        if node_id in CLIENT_SEND_EVENTS:
            CLIENT_SEND_EVENTS.pop(node_id, None)

    # 3. Cerrar la conexión (Fuera del lock, si es posible, para evitar bloqueos)
    if conn_to_close:
        try:
            # Si el scheduler llama a esta función, el socket podría estar cerrado.
            conn_to_close.shutdown(socket.SHUT_RDWR)
            conn_to_close.close()
            logging.info("[%s] Connection %s closed.", thread_name, node_id)
        except OSError as e:
            # Es normal si el socket ya está cerrado por el cliente.
            logging.debug("[%s] Socket %s already closed: %s", thread_name, node_id, e)
        except Exception as e:
            logging.error("[%s] Error closing the socket %s: %s", thread_name, node_id, e)
############################################################################################################



############################################################################################################
def main_server():
    """
    Función principal que inicia el servidor y los hilos.
    """

    # 1. Set up the CSV files
    setup_csv()

    # Start data saver thread
    csv_writer = threading.Thread(target=csv_writer_job, name="CSV-Writer")
    csv_writer.start()

    # 2. Starting the socket server
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allow to reuse the Port
            s.bind((HOST, PORT))
            s.listen(50)
            logger.info(f"📡 Server listening to {HOST}:{PORT}")

            # 3. Loop to accept connections
            s.settimeout(1.0) # Timeout to check STOP_EVENT
            while not STOP_EVENT.is_set():
                try:
                    conn, addr = s.accept()
                    # Start a new thread to handle the client
                    threading.Thread(target=handle_client, args=(conn, addr), name=f"{addr[1]}").start()

                except socket.timeout:
                    # Timeout to check if STOP_EVENT is being activated
                    continue
                except Exception as e:
                    logger.error(f"❌ Error accepting the connection: {e}")
                    break

    except Exception as e:
        logger.critical(f"❌ Fatal error starting the server: {e}")

    finally:
        logger.info("🛑 Stopped, waiting to ending...")
        STOP_EVENT.set()
        csv_writer.join()

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
############################################################################################################

