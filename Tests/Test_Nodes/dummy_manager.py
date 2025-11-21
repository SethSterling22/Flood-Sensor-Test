"""
This program is created to test the multi-node connection
and the synchronization with the server
"""


import os
import sys
import time
import json
import socket
import logging
import datetime
import threading
from dotenv import load_dotenv



# Import sensor functions
from dummy_node import get_data as dummy_data



# === ENVIRONMENT VARIABLES ===
load_dotenv("../../Env/.env.config")
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)


# === CONNETION SETTINGS ===
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
NODE_ID = "NODE_Dummy1"  # Must start with "NODE_"
# NODE_ID = f"NODE_{os.getenv('NODE_PREFIX', 'default')}"


# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'node_client.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# === GLOBAL VARIABLES ===
BUFFER_LOCK = threading.Lock()
SENSOR_DATA_BUFFER = [] 
CLIENT_READY = False
STOP_EVENT = threading.Event()
MIN_SEND_INTERVAL = 30
LAST_SENT_TIME = 0

# Just for DEBUGGING
counter = 0
counter_lock = threading.Lock()



# --- Thread Sensor Function ---
def listener_job(sensor_name, func):
    """
    Manages sensor data collection and 
    append it to the BUFFER.
    """

    global counter
    global CLIENT_READY
    logger.info("%s started.", sensor_name)

    # Receive the information from the Sensors
    while not STOP_EVENT.is_set():
        try:

            # CLIENT is READY when NODE_ID is received by the Sever
            if CLIENT_READY:
                # Call to the function
                data = func()
                with BUFFER_LOCK:
                    with counter_lock:
                        current_count = counter
                        counter = 0
                    now = datetime.datetime.now()
                    time_string = f"{now.hour}:{now.minute}:{now.second}"
                    SENSOR_DATA_BUFFER.append({
                        'sensor': sensor_name,
                        'timestamp': time_string,
                        'value': data,
                        'Counter': current_count
                    })
                    logger.debug("Buffered %s data: %.2f", sensor_name, data)
                # Wait for 60 seconds (the collection interval)
                STOP_EVENT.wait(60)

        except Exception as e:
            logger.error("%s thread error: %s", sensor_name, str(e))
            time.sleep(5)  # Wait before retrying


def counter_thread():
    global counter
    while not STOP_EVENT.is_set():
        time.sleep(1)  # Increment every second for tracking
        with counter_lock:
            counter += 1


def client():
    """
    Manages the connection and the messages from the server.
    """

    MAX_RETRY_COUNT = 3
    SHORT_WAIT_TIME = 15
    LONG_WAIT_TIME = 300  # 5 minutes
    retry_count = 0

    # Max ACK size (READY_TO_INDEX = 15)
    MAX_CMD_LEN = 15

    global CLIENT_READY
    while not STOP_EVENT.is_set():
        # 1. Try connection to server
        try:
            # "With" statements makes socket close automatically
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(300) 
                logger.info("📡 Connecting to %s:%d (Attempt %d/%d)", RECEIVER_HOST, RECEIVER_PORT, retry_count + 1, MAX_RETRY_COUNT)
                s.connect((RECEIVER_HOST, RECEIVER_PORT))

                # Wait until server connects and send the CONNECTED message
                # Leemos solo el máximo tamaño de comando para prevenir sticking
                response = s.recv(MAX_CMD_LEN).decode().strip()
                logger.info("📡 SERVER response on Connection: %s", response)

                # Check the connection message
                if response != "CONNECTED":
                    logger.error("⚠️ Error while connecting on server: %s", response)
                    raise socket.error(f"Server sent unexpected initial response: '{response}'")

                # Restart counter if is CONNECTED
                retry_count = 0

                # 2. Send the NODE_ID to index in the Server
                s.sendall(NODE_ID.encode('utf-8'))
                
                # Read the server message
                response_bytes = s.recv(MAX_CMD_LEN) 
                response = response_bytes.decode().strip()
                logger.info("📡 SERVER respond with: %s", response)

                
                if response.startswith("ID_RECEIVED"):
                    # --- CONNECTION STABLISHED AND NODE REGISTERED ---
                    CLIENT_READY = True
                    logger.info("✅ Connection established and ID registered. Starting data collection... 📊")

                    if len(response) > 11: # 'ID_RECEIVED' tiene 11 caracteres
                        logger.warning("⚠️ Extra command data received during registration: %s", response[11:])

                else:
                    logger.error("⚠️ NODE ID not indexed: %s", response)
                    raise socket.error(f"NODE ID not indexed. Server response: '{response}'")

                # 3. Principal receiver loop and data sending
                while not STOP_EVENT.is_set():
                    try:
                        # Waits one second to check STOP_EVENT
                        s.settimeout(1)

                        # Wait one minute for the READY_TO_INDEX from the server
                        try:
                            # Just read the 14 bytes of "READY_TO_INDEX"
                            message_bytes = s.recv(MAX_CMD_LEN) 

                            # If there's no bytes (timeout), retry
                            if not message_bytes:
                                continue
                            # Receive the message
                            message = message_bytes.decode().strip()

                        except socket.timeout:
                            continue

                        # If server is ready to index:
                        if message.startswith("READY_TO_INDEX"):
                            logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")
                            
                            # Get and clean BUFFERED data
                            with BUFFER_LOCK:
                                data_to_send = SENSOR_DATA_BUFFER.copy()

                            # --- CONSTRUCCIÓN DEL PAYLOAD (Lógica correcta de longitud-prefijo) ---
                            if data_to_send:
                                # 1. Serializar data estructurada a CADENA JSON (str)
                                try:
                                    payload_str = json.dumps(data_to_send)
                                    logger.info("📤 Sending %s data points.", len(data_to_send))
                                    logger.info("DATA sent:\n %s", data_to_send)
                                except TypeError as e:
                                    logger.error("⚠️ Error serializing JSON. Check data format: %s", e)
                                    # Al no limpiar el buffer, se reintenta en el próximo ciclo
                                    break 

                            else:
                                # 2. ESCENARIO 'NO_DATA' (str)
                                logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
                                payload_str = "NO_DATA"

                            # 3. CODIFICATION AND PREPARATION OF LENGTH PROTOCOL 
                            payload_bytes = payload_str.encode('utf-8')
                            payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')

                            # 4. Send (Length + Payload)
                            s.sendall(payload_length_bytes)
                            s.sendall(payload_bytes)        

                            # 5. Wait Server confirmation
                            s.settimeout(60)
                            ack_bytes = s.recv(MAX_CMD_LEN)
                            ack = ack_bytes.decode().strip()
                            s.settimeout(1)

                            if ack == "DATA_RECEIVED":
                                logger.info("👍 Data successfully indexed by server.")
                                # Just clean the buffer if the Data was received
                                with BUFFER_LOCK:
                                    SENSOR_DATA_BUFFER.clear()

                            elif ack.startswith("DATA_RECEIVED"):
                                logger.warning("⚠️ Received concatenated ACK: %s", ack)
                                with BUFFER_LOCK:
                                    SENSOR_DATA_BUFFER.clear()


                            elif ack == "JSON_ERROR":
                                logger.error("❌ Server failed decoding JSON data. The data was not saved.")
                                break
                            else:
                                logger.error("❌ Server ACK error: %s", ack)
                                break

                        elif message:
                            logger.warning("Received unknown message: %s", message)

                    except ConnectionResetError:
                        logger.error("🚫 Connection lost (Server closed the connection).")
                        break
                    except Exception as e:
                        logger.error("🔌 Fatal error during communication: %s", e)
                        break

        except socket.error as e:
            logger.error("❌ Failed to connect to server: %s", e)
            CLIENT_READY = False 

            # Increase the counter
            retry_count += 1

            if retry_count <= MAX_RETRY_COUNT:
                logger.info("⏳ Waiting %d seconds before next retry...", SHORT_WAIT_TIME)
                time.sleep(SHORT_WAIT_TIME)
            else:
                logger.info("😴 Failed %d times. Waiting %d seconds (5 minutes) before resetting attempts...", 
                            MAX_RETRY_COUNT, LONG_WAIT_TIME)
                time.sleep(LONG_WAIT_TIME)
                retry_count = 0 

    CLIENT_READY = False
    logger.info("🔌 Client thread terminated.")


if __name__ == "__main__":
    """
    Work with thread synchronization, start and
    end the program.
    """

    # Sensor Start
    sensors = [
        threading.Thread(target=listener_job, args=("Rain Gauge", dummy_data)),
        threading.Thread(target=listener_job, args=("Flood Sensor", dummy_data)),

        threading.Thread(target=counter_thread)
    ]

    for sensor in sensors:
        sensor.start()

    # Start Client on thread to do not block main
    client_thread = threading.Thread(target=client)
    client_thread.start()

    try:
        while True:
            time.sleep(1) # Principal thread waits
    except KeyboardInterrupt:
        logger.info("🛑 Stopping all threads...")
        STOP_EVENT.set()
    finally:
        for sensor in sensors:
            sensor.join()

        client_thread.join() # Wait until the client stop
        logger.info("👋 All threads stopped")
        sys.exit(0)


#################################################################################################################3

#DATA_BUFFER: Dict[str, Any] = {}
#DATA_LOCK = threading.Lock()
# STOP_EVENT = threading.Event()
# CONNECTION_READY_EVENT = threading.Event()

# class NodeClient:
#     def __init__(self):
#         self.socket = None
#         self.connection_thread = None
#         self.last_minute_sync = None

#     def establish_connection(self):
#         """Establish and maintain persistent connection to server"""
#         while not STOP_EVENT.is_set():
#             try:
#                 self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 self.socket.settimeout(30)
#                 self.socket.connect((RECEIVER_HOST, RECEIVER_PORT))

#                 # Initial handshake
#                 id_request = self.socket.recv(1024)
#                 if id_request != "NODE_ID_REQUEST":
#                     raise ConnectionError("Invalid protocol initiation")

#                 self.socket.sendall(NODE_ID.encode('utf-8'))
#                 response = self.socket.recv(1024)

#                 if response != "READY":
#                     raise ConnectionError("Server not ready")

#                 logger.info("🟢 Connection established and READY")
#                 CONNECTION_READY_EVENT.set()
#                 self.socket.settimeout(60)  # Set to minute sync timeout

#                 # Main connection loop
#                 while not STOP_EVENT.is_set():
#                     try:
#                         # Wait for server's minute sync signal
#                         signal = self.socket.recv(1024)
#                         if signal == "READY":
#                             self.handle_minute_sync()
#                         elif not signal:
#                             raise ConnectionError("Server disconnected")

#                     except socket.timeout:
#                         logger.warning("⏰ Minute sync timeout, reconnecting...")
#                         break

#             except (ConnectionError, socket.error) as e:
#                 logger.error(f"🔌 Connection error: {str(e)}")
#                 CONNECTION_READY_EVENT.clear()
#                 self.cleanup_connection()
#                 if not STOP_EVENT.wait(5):
#                     continue
#             except Exception as e:
#                 logger.error(f"🔴 Unexpected error: {str(e)}")
#                 CONNECTION_READY_EVENT.clear()
#                 self.cleanup_connection()
#                 if not STOP_EVENT.wait(10):
#                     continue

#     def handle_minute_sync(self):
#         """Handle minute synchronization and data transmission"""
#         logger.info("⏰ Minute sync received from server")

#         # Collect data from both sensors
#         with DATA_LOCK:
#             DATA_BUFFER["🌧️ Rain Gauge"] = dummy_data()
#             DATA_BUFFER["💧 Flood Sensor"] = dummy_data()

#             payload = {
#                 "node_id": NODE_ID,
#                 "timestamp": datetime.now().isoformat(),
#                 "metrics": DATA_BUFFER.copy()
#             }
#             DATA_BUFFER.clear()

#         # Send data to server
#         try:
#             self.socket.sendall(json.dumps(payload).encode('utf-8'))
#             response = self.socket.recv(1024)

#             if response == "OK_QUEUED":
#                 logger.info("✅ Data successfully queued at server")
#             else:
#                 logger.warning(f"⚠️ Unexpected server response: {response}")

#         except Exception as e:
#             logger.error(f"🔴 Failed to send data: {str(e)}")
#             raise ConnectionError("Data transmission failed")

#     def cleanup_connection(self):
#         """Clean up socket connection"""
#         if self.socket:
#             try:
#                 self.socket.close()
#             except:
#                 pass
#             self.socket = None

# def sensor_collector(sensor_name: str):
#     """Background thread to collect sensor data"""
#     while not STOP_EVENT.is_set():
#         CONNECTION_READY_EVENT.wait()  # Only collect when connected

#         try:
#             data = dummy_data()  # Replace with actual sensor function

#             with DATA_LOCK:
#                 DATA_BUFFER[sensor_name] = data
#                 logger.info(f"📊 Collected {sensor_name} data: {data}")

#         except Exception as e:
#             logger.error(f"❌ {sensor_name} collection error: {str(e)}")



# def main():
#     node = NodeClient()

#     # Start connection manager thread
#     connection_thread = threading.Thread(target=node.establish_connection)
#     connection_thread.daemon = True
#     connection_thread.start()

#     # Start sensor collector threads
#     sensors = [
#         threading.Thread(target=sensor_collector, args=("🌧️ Rain Gauge",)),
#         threading.Thread(target=sensor_collector, args=("💧 Flood Sensor",))
#     ]

#     for sensor in sensors:
#         sensor.daemon = True
#         sensor.start()

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("🛑 Shutting down node...")
#         STOP_EVENT.set()
#         node.cleanup_connection()
#         connection_thread.join()
#         for sensor in sensors:
#             sensor.join()
#         logger.info("👋 Node shutdown complete")

# if __name__ == "__main__":
#     main()

###########################################################################


