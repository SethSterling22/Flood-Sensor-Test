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
from metrics_uploader import run_uploader
from utils import get_next_hourly_filename, job_submission_thread



##################################################################################################
# ====== ENVIRONMENT VARIABLES ======
load_dotenv("./Env/.env.config")
HOST = "0.0.0.0" # All transmitters
PORT = int(os.getenv("RECEIVER_PORT") or 4040)


# ====== GLOBAL VARIABLES AND LOCKS ======
CLIENTS_INDEX = {}
CLIENT_SEND_EVENTS = {}
CLIENT_SEND_READY_FLAGS = {}
INDEX_LOCK = threading.Lock() # For the clients to start index
STOP_EVENT = threading.Event()
CSV_WRITE_QUEUE = queue.Queue() # Thread for the CSV writing while handling other data
ROTATION_LOCK = threading.Lock()
CLIENT_FLAG_LOCK = threading.Lock()


# ====== SAVE FILES PATH ======
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)
CSV_DIR = os.path.join(LOG_DIR,"Water_data/")

# Provides the filename
os.makedirs(CSV_DIR, exist_ok=True)
initial_filename = get_next_hourly_filename()
CSV_FILE = os.path.join(CSV_DIR, initial_filename)
SENSOR_FILE = os.path.join(CSV_DIR, "metrics_template.csv")


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
##################################################################################################



##################################################################################################
# ====== SETUP CSV ======
def setup_csv(filename):
    """
    Create the CSV with the given name and add the header
    """
    try:
        if not os.path.exists(SENSOR_FILE):
            with open(SENSOR_FILE, "w", newline="", encoding='utf-8-sig') as file:
                writer = csv.writer(file, delimiter="\t")
                writer.writerow(["alias,variablename,postprocess,units,datatype"]) # Default fields

                # Fields to upload
                writer.writerow(["Precipitation,Precipitation,False,mm,float"])
                writer.writerow(["Temperature,Temperature,False,Celsius,float"])
                writer.writerow(["Humidity,Humidity,False,Percentage,float"])
                writer.writerow(["Flooding,Flooding,False,boolean,integer"])

            logger.info("✅ Created sensor file at %s", SENSOR_FILE)
        else:
            logger.info("Sensor file exists at %s", SENSOR_FILE)
    except Exception as e:
        logger.error("Failed to create sensor file: %s", e)

    try:
        if not os.path.exists(filename):
            with open(filename, mode='w', newline='', encoding='utf-8-sig') as file:
                # write on the CSV the data
                # CSV FORMAT: | Precipitation | DTemperatureegrees | Humidity | Flooding | Node_Id | Station_Id | collectiontime | Lat_deg | Lon_deg |
                csv.writer(file).writerow(['Precipitation', 'Temperature', 'Humidity', 'Flooding', 'Node_Id', 'Station_Id', 'collectiontime', "Lat_deg", "Lon_deg"])

            logger.info("💾 File data %s ready with headers.", filename)
        else:
            logger.info("💾 File data %s already exists.", filename)
        return True
    except Exception as e:
        logger.error("❌ ERROR setting up CSV: %s", e)
        return False


def extract_and_flatten_data(node_id, timestamp, data_list):
    """
    Extract and aggregate the data fields into a single row 
    based on the new fixed CSV format (Precipitation, Degrees, Flooding)
    """

    # 1. Initialize values
    precipitation = None
    temp_value = None
    humidity_value = None
    flooding = None

    for data_item in data_list:

        # 2. Buffer data extraction and metadata extraction from "data_item"
        sensor_name = data_item.get("Sensor") or data_item.get("sensor")
        raw_value = data_item.get("Value") or data_item.get("value")

        # Extract metadata
        station_id = data_item.get("Station_Id") or data_item.get("station_id")
        lat_deg = data_item.get("Lat_deg") or data_item.get("lat_deg")
        lon_deg = data_item.get("Lon_deg") or data_item.get("lon_deg")

        if sensor_name is None:
            logger.warning("⚠️ Item del buffer incompleto, saltando: %s", data_item)
            return []

        # 3. Filter by Sensor Name and assign the value to a variable
        if sensor_name == "Rain Gauge":
            print("DEBUG VALUE")
            print(raw_value)
            precipitation = raw_value

        elif sensor_name == "Temperature and Humidity" and isinstance(raw_value, (list, tuple)) and len(raw_value) >= 2:
            temp_value = raw_value[0]
            humidity_value = raw_value[1]

        elif sensor_name == "Flood Sensor":
            flooding = raw_value

            # 4. Check flooding and activate the job subission thread
            if flooding == 1:
                logger.info("🚨 Flood detected! Submitting job for processing.")
                # Call to the Job submission
                job_submission_thread()
        else:
            # Sensor doesn't have a column, ignore
            logger.warning("⚠️ Unmapped sensor (%s) ignored in normalization to fixed CSV format.", sensor_name)
            return []

    # 5. Row construction
    # CSV ORDER: ['Precipitation', 'Degrees', 'Flooding', 'Node_Id', 'Station_Id', 'collectiontime', 'Lat_deg', 'Lon_deg']
    complete_row = [precipitation, temp_value, humidity_value, flooding, node_id, station_id, timestamp, lat_deg, lon_deg]

    return complete_row


# ====== CSV WRITER ======
def csv_writer_job():
    """
    Thread dedicated to consuming tasks from the
    CSV_WRITE_QUEUE and writing them to the file.
    """

    last_upload = time.time()
    upload_interval = 3600 # 1 hour
    #upload_interval = 600 # 10 minutes to debbug
    global CSV_FILE

    logger.info("📝 CSV Writer thread started.")
    while not STOP_EVENT.is_set():
        try:
            # Espera por datos en la cola por 1 segundo
            item = CSV_WRITE_QUEUE.get(timeout=1)
            data_list, node_id = item # data_list contains the dictionary to plain

            # 1. Process and writing
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            complete_row = []

            # Call the function to take the data clean and formatted
            try:
                complete_row = extract_and_flatten_data(node_id, now, data_list)
            except Exception as e:
                logger.error("❌ Error during data plain: %s for item: %s", e, data_list)

            # for data_item in data_list:
            #     try:
            #         flattened_rows = extract_and_flatten_data(node_id, now, data_item)
            #         complete_row.extend(flattened_rows)
            #     except Exception as e:
            #         logger.error("❌ Error during data plain: %s for item: %s", e, data_item)

            try:
                # Write in file just if there are valid rows
                if complete_row:
                    with ROTATION_LOCK:
                        current_csv_file = CSV_FILE

                    with open(current_csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
                        csv.writer(file).writerow(complete_row)
                    logger.info("💾 Saved %d ítems in final format from %s to %s", len(complete_row), os.path.basename(current_csv_file), node_id)
                else:
                    logger.warning("⚠️ Valid rows were not generated to write. Data discard for %s.", node_id)
                CSV_WRITE_QUEUE.task_done()

            except OSError as e:
                logger.error("❌ OS/File Error [%d]: %s. Data RE-QUEUED for safety.", e.errno, e.strerror)
                CSV_WRITE_QUEUE.put(item)
                if not STOP_EVENT.wait(10):
                    continue
                else:
                    break

            except Exception as e:
                logger.error("❌ General I/O Error: %s", e)
                CSV_WRITE_QUEUE.task_done()

        # UPLOAD SECTION
        except queue.Empty:
            # 2. Rotate and upload
            if time.time() - last_upload >= upload_interval:
                with ROTATION_LOCK:
                    file_to_upload = CSV_FILE
                logger.info("🔄 Rotation hour: Trying to start uploading of %s", os.path.basename(file_to_upload))

                try:
                    # Call to uploader IMPORTANT, previous file will be deleted?
                    uploader_metrics(file_to_upload) # <-- Here
                    last_upload = time.time()

                except Exception as e:
                    logger.error("❌ Thread start failed: %s", str(e))
            continue

        except Exception as e:
            logger.error("❌ Error processing CSV queue task: %s", e)

            # Clean and continue as default
            if 'item_data' in locals():
                CSV_WRITE_QUEUE.task_done()

    logger.info("📝 CSV Writer thread terminated.")


def handle_upload_and_rotation(file_to_upload):
    """
    Function executed in a separate thread.
    It calls the uploader to process the file and, if successful, 
    rotates the global variable.
    """

    global CSV_FILE

    # 1. Execute upload and clean
    upload_success = run_uploader(file_to_upload)

    # 2. Rotation: Generate and create new file
    try:
        new_filename_base = get_next_hourly_filename()
        new_csv_file_path = os.path.join(os.path.dirname(file_to_upload), new_filename_base)

        if setup_csv(new_csv_file_path):
            # 3. Update global variable of secure way
            with ROTATION_LOCK:
                CSV_FILE = new_csv_file_path
            logger.info("✅ Rotation successful. New active file.: %s", os.path.basename(CSV_FILE))

            if not upload_success:
                # Si falla, el archivo antiguo (file_to_upload) se mantiene para un reintento futuro.
                logger.warning("⚠️ Upload failed, but rotation successful. Data is now being written to the new file: %s", os.path.basename(CSV_FILE))
                logger.warning("Archivo fallido mantenido para reintento: %s", os.path.basename(file_to_upload))

            return True
        logger.error("❌ Failure to create new CSV file during rotation.")
        return False

    except Exception as e:
        logger.error("❌ Error during file rotation: %s", str(e))
        return False


def uploader_metrics(file_to_upload):
    """
    Start new thread for the upload and rotation (non blocking)
    """
    upload_thread = threading.Thread(target=lambda: handle_upload_and_rotation(file_to_upload), name=f"RotationThread-{os.path.basename(file_to_upload)}")
    upload_thread.start()

    logger.info("⬆️ Upload thread started for: %s . Writer continues monitoring.", os.path.basename(file_to_upload))
    return True
##################################################################################################



##################################################################################################
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
        conn.settimeout(45)
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
                return # Clean Exit
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
                conn.settimeout(80)

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
                # except:
                #     pass
                except Exception:
                    safe_cleanup(node_id)

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
                logger.warning("[%s] Client failed data reception: %s. Cleaning up: \n %s", thread_name, node_id, e)
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
            except Exception:
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
##################################################################################################



##################################################################################################
def main_server():
    """
    Función principal que inicia el servidor y los hilos.
    """

    # 1. Set up the CSV files
    setup_csv(CSV_FILE)

    # Start data saver thread
    csv_writer = threading.Thread(target=csv_writer_job, name="CSV-Writer")
    csv_writer.start()

    # 2. Starting the socket server
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allow to reuse the Port
            s.bind((HOST, PORT))
            s.listen(50)
            logger.info("📡 Server listening to %s : %d", HOST, PORT)

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
                    logger.error("❌ Error accepting the connection: %s", e)
                    break

    except Exception as e:
        logger.critical("❌ Fatal error starting the server: %s", e)

    finally:
        logger.info("🛑 Stopped, waiting to ending...")
        STOP_EVENT.set()
        csv_writer.join()

        # Close all active connection
        with INDEX_LOCK:
            # for node_id, conn in CLIENTS_INDEX.items():
            for conn in CLIENTS_INDEX.values():
                try:
                    conn.close()
                except Exception:
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
##################################################################################################

