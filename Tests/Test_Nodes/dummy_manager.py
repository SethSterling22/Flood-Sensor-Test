"""
This program is created to test the multi-node connection
and the synchronization with the server
"""


import os
import sys
import time
import json
import socket
import random
import logging
import threading
from dotenv import load_dotenv



# Import sensor functions
from dummy_node import get_data as dummy_data



# ====== ENVIRONMENT VARIABLES ======
load_dotenv("../../Env/.env.config")
load_dotenv("../../Env/.env.public")
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)


# ====== CONNETION SETTINGS ======
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
NODE_ID = "NODE_Dummy1"  # Must start with "NODE_"
# NODE_ID = f"NODE_{os.getenv('STATION_NAME', 'default')}"


# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'node_client.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ====== GLOBAL VARIABLES ======
BUFFER_LOCK = threading.Lock()
SENSOR_DATA_BUFFER = [] 
CLIENT_READY = False
STOP_EVENT = threading.Event()
MIN_SEND_INTERVAL = 30
LAST_SENT_TIME = 0
LATITUDE = os.getenv('GPS_LAT')
LONGITUDE = os.getenv('GPS_LON')
STATION_ID = int(os.getenv('STATION_ID'))

# Just for DEBUGGING
counter = 0
counter_lock = threading.Lock()



# ====== Thread Sensor Function ======
##############################################################################################
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
        start_time = time.time()
        try:

            # CLIENT is READY when NODE_ID is received by the Sever
            if CLIENT_READY:
                # Call to the function
                data = func()
                with BUFFER_LOCK:
                    with counter_lock:
                        current_count = counter
                        counter = 0
                    SENSOR_DATA_BUFFER.append({
                        'Sensor': sensor_name,
                        'Value': data,
                        'Station_Id': STATION_ID,
                        'Lat_deg': LATITUDE,
                        'Lon_deg': LONGITUDE
                    })
                    logger.debug("Buffered %s data: %.2f", sensor_name, data)

                # Wait for 60 seconds (the collection interval)
                elapsed = time.time() - start_time
                sleep_time = max(0, 55.0 - elapsed)

                if STOP_EVENT.wait(sleep_time):
                    # if STOP_EVENT break
                    break

        except Exception as e:
            logger.error("%s thread error: %s", sensor_name, str(e))
            time.sleep(5)  # Wait before retrying
##############################################################################################



def counter_thread():
    global counter
    while not STOP_EVENT.is_set():
        time.sleep(1)  # Increment every second for tracking
        with counter_lock:
            counter += 1



# ====== Thread Client Function ======
##############################################################################################
def client():
    """
    Manages the connection and the messages to the server.
    """

    MAX_RETRY_COUNT = 4
    SHORT_WAIT_TIME = 20
    LONG_WAIT_TIME = 180  # 3 minutes
    retry_count = 0

    global CLIENT_READY
    while not STOP_EVENT.is_set():
        # 1. Try connection to server
        try:
            # Open socket and prepares to receive messages
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10) 
                logger.info("📡 Connecting to %s:%d (Attempt %d/%d)", RECEIVER_HOST, RECEIVER_PORT, retry_count + 1, MAX_RETRY_COUNT)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.connect((RECEIVER_HOST, RECEIVER_PORT))

                s.settimeout(45) 
                # Wait until server connects and send the CONNECTED message
                response = s.recv(9).decode().strip()
                logger.info("📡 SERVER response on Connection: %s", response)

                # Check the connection message
                if response != "CONNECTED":
                    logger.error("⚠️ Error while connecting on server: %s", response)
                    raise socket.error(f"Server sent unexpected initial response: '{response}'")

                # Restart counter if CONNECTED
                retry_count = 0

                # 2. Send the NODE_ID to index in the Server
                s.sendall(NODE_ID.encode('utf-8'))

                # Read the server message
                response_bytes = s.recv(11) 
                response = response_bytes.decode().strip()
                logger.info("📡 SERVER respond with: %s", response)

                # Connection stablished and Node registered 
                if response.startswith("ID_RECEIVED"):
                    CLIENT_READY = True
                    logger.info("✅ Connection established and ID registered. Starting data collection... 📊")

                else:
                    logger.error("⚠️ NODE ID not indexed: %s", response)
                    raise socket.error(f"NODE ID not indexed. Server response: '{response}'")

                # 3. PRINCIPAL LOOP AND DATA SENDING
                while not STOP_EVENT.is_set():
                    try:
                        # Waits one second to check STOP_EVENT
                        s.settimeout(90)

                        message_bytes = s.recv(14)
                        if not message_bytes:
                            logger.error("🚫 Server closed the connection while waiting for signal.")
                            break
                        message = message_bytes.decode().strip()

                        # If server is ready to index:
                        if message.startswith("READY_TO_INDEX"):
                            logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")

                            try:
                                s.settimeout(0.1)
                                drain_bytes = s.recv(30) 
                                if drain_bytes:
                                    logger.warning("🌊 Drained residual signal after ID_RECEIVED: %s", drain_bytes.decode().strip())

                            except socket.timeout:
                                # OK, socket limpio
                                pass
                            except Exception as e:
                                logger.debug("Error draining buffer after ID_RECEIVED: %s", e)

                            # 3. CODIFICATION AND PREPARATION OF LENGTH PROTOCOL 
                            with BUFFER_LOCK:
                                # If buffer is empty, send "NO_DATA"
                                if not SENSOR_DATA_BUFFER:
                                    data_to_send = []
                                    logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
                                else:
                                    # Just send the data if the BUFFER is not empty
                                    # Copy the buffer
                                    data_to_send = SENSOR_DATA_BUFFER.copy()
                                    logger.info("📤 Sending %s data points.", len(data_to_send))
                                    logger.info("DATA sent:\n %s", data_to_send)

                            # PAYLOAD BUILDING 
                            if not data_to_send:
                                payload_str = "NO_DATA"
                            else:
                                try:
                                    payload_str = json.dumps(data_to_send)
                                except TypeError as e:
                                    logger.error("⚠️ Error serializing JSON. Check data format: %s. Data not sent.", e)
                                    break

                            
                            payload_bytes = payload_str.encode('utf-8')
                            payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')

                            try: 
                                # 4. SEND LENGTH & PAYLOAD (Short timeout to write: 15s)
                                full_payload = payload_length_bytes + payload_bytes
                                s.settimeout(35) 
                                s.sendall(full_payload) 

                                # 5. WAIT SERVER CONFIRMATION (ACK)
                                s.settimeout(50) 
                                ack_bytes = s.recv(13)
                                ack = ack_bytes.decode('utf-8').strip()

                                if not ack_bytes:
                                    logger.error("🚫 Server closed connection unexpectedly after data submission.")
                                    break 

                                if not ack:
                                    logger.error("❌ Server ACK error receiving data: ACK received was empty or corrupted.")
                                    break

                                # ACK processing logic
                                if ack.startswith("DATA_RECEIVED"):
                                    # Success: Server confirmed reception
                                    logger.info("👍 Data successfully indexed by server. [%s]", ack)

                                    # Clean BUFFER just if the delivery was successful
                                    with BUFFER_LOCK:
                                        SENSOR_DATA_BUFFER.clear()
                                    # Go back to the start for the next READY_TO_INDEX signal
                                    continue 

                                elif ack == "JSON_ERROR":
                                    logger.error("❌ Server failed decoding JSON data. The data was not saved.")
                                    break

                                elif ack.startswith("READY_TO_INDEX"):
                                    # Server too fast, ACK desynchronized
                                    logger.warning("⚠️ Desynchronization: Received READY_TO_INDEX instead of ACK. Reconnecting to sync.")
                                    break 

                                else:
                                    logger.error("❌ Server ACK error receiving data: %s", ack)
                                    break

                            except socket.timeout:
                                # If server doesn't respond the ACK on time
                                logger.error("❌ Timeout waiting for server ACK (50s). Disconnecting to retry.")
                                break

                            except socket.error as se: 
                                # Capture BrokenPipeError, ConnectionResetError
                                logger.error("❌ Socket error during send/ACK (%s). Disconnecting to retry.", se)
                                break

                            except Exception as e:
                                logger.error("🔌 Unexpected error during data transfer: %s", e)
                                break

                        elif message.startswith("DATA_RECEIVED"):
                            # Caso donde llegó ACK de datos previos
                            logger.info("Received delayed DATA_RECEIVED, continuing...")
                            continue

                    except socket.timeout:
                        continue
                    except ConnectionResetError:
                        logger.error("🚫 Connection lost (Server closed the connection).")
                        break
                    except Exception as e:
                        logger.error("🔌 Fatal error during communication: %s", e)
                        break

        except socket.error as e:
            logger.error("❌ Failed to connect to server: %s", e)
            CLIENT_READY = False
            time.sleep(2)

            # Increase the counter
            retry_count += 1

            if retry_count < MAX_RETRY_COUNT:
                reconnect_time = SHORT_WAIT_TIME + random.uniform(0, 5)
                logger.info("⏳ Waiting %d seconds before next retry...", reconnect_time)
                if STOP_EVENT.wait(reconnect_time):
                    # if STOP_EVENT break
                    break
            else:
                logger.info("😴 Failed %d times. Waiting %d seconds before resetting attempts...", MAX_RETRY_COUNT, LONG_WAIT_TIME)
                if STOP_EVENT.wait(LONG_WAIT_TIME):
                    # if STOP_EVENT break
                    break
                retry_count = 0 

    CLIENT_READY = False
    logger.info("🔌 Client thread terminated.")
##############################################################################################



##############################################################################################
if __name__ == "__main__":
    # Start Client on thread to do not block main
    client_thread = threading.Thread(target=client)
    client_thread.start()

    # Sensor Start
    sensors = [
        threading.Thread(target=listener_job, args=("Rain Gauge", dummy_data)),
        threading.Thread(target=listener_job, args=("Flood Sensor", dummy_data)),

        threading.Thread(target=counter_thread)
    ]

    for sensor in sensors:
        sensor.start()

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
##############################################################################################



















# # --- Thread Sensor Function ---
# def listener_job(sensor_name, func):
#     """
#     Manages sensor data collection and 
#     append it to the BUFFER.
#     """

#     global counter
#     global CLIENT_READY
#     logger.info("%s started.", sensor_name)

#     # Receive the information from the Sensors
#     while not STOP_EVENT.is_set():
#         try:

#             # CLIENT is READY when NODE_ID is received by the Sever
#             if CLIENT_READY:
#                 # Call to the function
#                 data = func()
#                 with BUFFER_LOCK:
#                     with counter_lock:
#                         current_count = counter
#                         counter = 0
#                     now = datetime.datetime.now()
#                     time_string = f"{now.hour}:{now.minute}:{now.second}"
#                     SENSOR_DATA_BUFFER.append({
#                         'sensor': sensor_name,
#                         'timestamp': time_string,
#                         'value': data,
#                         'Counter': current_count
#                     })
#                     logger.debug("Buffered %s data: %.2f", sensor_name, data)
#                 # Wait for 60 seconds (the collection interval)
#                 STOP_EVENT.wait(50)
#             else:   
#                 STOP_EVENT.wait(5)

#         except Exception as e:
#             logger.error("%s thread error: %s", sensor_name, str(e))
#             time.sleep(5)  # Wait before retrying



# def counter_thread():
#     global counter
#     while not STOP_EVENT.is_set():
#         time.sleep(1)  # Increment every second for tracking
#         with counter_lock:
#             counter += 1


# def client():
#     """
#     Manages the connection and the messages from the server.
#     """

#     MAX_RETRY_COUNT = 4
#     SHORT_WAIT_TIME = 15 + random.uniform(0, 5)
#     LONG_WAIT_TIME = 300  # 5 minutes
#     retry_count = 0

#     global CLIENT_READY
#     while not STOP_EVENT.is_set():
#         # 1. Try connection to server
#         try:
#             # "With" statements makes socket close automatically
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.settimeout(35) 
#                 logger.info("📡 Connecting to %s:%d (Attempt %d/%d)", RECEIVER_HOST, RECEIVER_PORT, retry_count + 1, MAX_RETRY_COUNT)
#                 s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#                 s.connect((RECEIVER_HOST, RECEIVER_PORT))
                

#                 # Wait until server connects and send the CONNECTED message
#                 response = s.recv(20).decode().strip()
#                 logger.info("📡 SERVER response on Connection: %s", response)

#                 # Check the connection message
#                 if response != "CONNECTED":
#                     logger.error("⚠️ Error while connecting on server: %s", response)
#                     raise socket.error(f"Server sent unexpected initial response: '{response}'")

#                 # Restart counter if is CONNECTED
#                 retry_count = 0

#                 # 2. Send the NODE_ID to index in the Server
#                 s.sendall(NODE_ID.encode('utf-8'))
                
#                 # Read the server message
#                 response_bytes = s.recv(20) 
#                 response = response_bytes.decode().strip()
#                 logger.info("📡 SERVER respond with: %s", response)


#                 # === CONNECTION STABLISHED AND NODE REGISTERED ===
#                 if response.startswith("ID_RECEIVED"):
#                     CLIENT_READY = True
#                     logger.info("✅ Connection established and ID registered. Starting data collection... 📊")

#                 else:
#                     logger.error("⚠️ NODE ID not indexed: %s", response)
#                     raise socket.error(f"NODE ID not indexed. Server response: '{response}'")
#                 # === CONNECTION STABLISHED AND NODE REGISTERED ===


#                 # 3. Principal receiver loop and data sending
#                 while not STOP_EVENT.is_set():
#                     try:
#                         # Waits one second to check STOP_EVENT
#                         s.settimeout(65)
        
#                         message_bytes = s.recv(20)
#                         if not message_bytes:
#                             continue
                            
#                         message = message_bytes.decode().strip()


#                         # If server is ready to index:
#                         if message.startswith("READY_TO_INDEX"):
#                             logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")



#                             try:
#                                 s.settimeout(0.05) # Timeout muy corto (10 ms)
#                                 drain_bytes = s.recv(30) 
#                                 if drain_bytes:
#                                     logger.warning("🌊 Drained residual signal after ID_RECEIVED: %s", drain_bytes.decode().strip())
                                    
#                             except socket.timeout:
#                                 # OK, socket limpio
#                                 pass
#                             except Exception as e:
#                                 logger.debug("Error draining buffer after ID_RECEIVED: %s", e)






#                             with BUFFER_LOCK:
#                                 # 2. If buffer is empty, send "NO_DATA"
#                                 if not SENSOR_DATA_BUFFER:
#                                     data_to_send = []
#                                     logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
#                                 else:
#                                     # Just send the data if the BUFFER is not empty
#                                     # Copy the buffer
#                                     data_to_send = SENSOR_DATA_BUFFER.copy()
#                                     logger.info("📤 Sending %s data points.", len(data_to_send))
#                                     logger.info("DATA sent:\n %s", data_to_send)

#                             # PAYLOAD BUILDING 
#                             if not data_to_send:
#                                 payload_str = "NO_DATA"
#                             else:
#                                 try:
#                                     payload_str = json.dumps(data_to_send)
#                                 except TypeError as e:
#                                     logger.error("⚠️ Error serializing JSON. Check data format: %s. Data not sent.", e)
#                                     break

#                             # 3. CODIFICATION AND PREPARATION OF LENGTH PROTOCOL 
#                             payload_bytes = payload_str.encode('utf-8')
#                             payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')

#                             # -------------------------------------------------------------
#                             # 4. Send (Length + Payload) & 5. Wait Server confirmation (ACK)
#                             # -------------------------------------------------------------
#                             try: 
#                                 # 4. Send data (Short timeout to write: 15s)
#                                 full_payload = payload_length_bytes + payload_bytes
#                                 s.settimeout(35) 
#                                 s.sendall(full_payload) 

#                                 # 5. Wait for confirmation
#                                 s.settimeout(35) 
#                                 ack_bytes = s.recv(20)
#                                 ack = ack_bytes.decode('utf-8').strip()
#                                 s.settimeout(1)
#                                 # ########## ACK processing logic ##########
#                                 if ack.startswith("DATA_RECEIVED"):
#                                     # Success: Server confirmed reception
#                                     logger.info("👍 Data successfully indexed by server. [%s]", ack)
                                    
#                                     # Clean BUFFER just if the delivery was successful
#                                     with BUFFER_LOCK:
#                                         SENSOR_DATA_BUFFER.clear()
                                        

#                                     # Go back to the start for the next READY_TO_INDEX signal
#                                     continue 

#                                 elif ack == "JSON_ERROR":
#                                     logger.error("❌ Server failed decoding JSON data. The data was not saved.")
#                                     break

#                                 elif ack.startswith("READY_TO_INDEX"):
#                                     # Server too fast, ACK desynchronized
#                                     logger.warning("⚠️ Desynchronization: Received READY_TO_INDEX instead of ACK. Reconnecting to sync.")
#                                     break 

#                                 else:
#                                     logger.error("❌ Server ACK error receiving data: %s", ack)
#                                     break
                                
#                                 s.settimeout(35) 

#                             except socket.timeout:
#                                 # If server doesn't respond the ACK on time
#                                 logger.error("❌ Timeout waiting for server ACK (35s). Disconnecting to retry.")
#                                 break

#                             except socket.error as se: 
#                                 # Capture BrokenPipeError, ConnectionResetError
#                                 logger.error("❌ Socket error during send/ACK (%s). Disconnecting to retry.", se)
#                                 break

#                             except Exception as e:
#                                 logger.error("🔌 Unexpected error during data transfer: %s", e)
#                                 break

#                         elif message.startswith("DATA_RECEIVED"):
#                             # Caso donde llegó ACK de datos previos
#                             logger.info("Received delayed DATA_RECEIVED, continuing...")
#                             continue

#                     except socket.timeout:
#                         continue
#                     except ConnectionResetError:
#                         logger.error("🚫 Connection lost (Server closed the connection).")
#                         break
#                     except Exception as e:
#                         logger.error("🔌 Fatal error during communication: %s", e)
#                         break
        
#         except socket.error as e:
#             logger.error("❌ Failed to connect to server: %s", e)
#             CLIENT_READY = False
#             time.sleep(2)
            
#             # Increase the counter
#             retry_count += 1
            
#             if retry_count < MAX_RETRY_COUNT:
#                 logger.info("⏳ Waiting %d seconds before next retry...", SHORT_WAIT_TIME)
#                 time.sleep(SHORT_WAIT_TIME)
#             else:
#                 logger.info("😴 Failed %d times. Waiting %d seconds (5 minutes) before resetting attempts...", 
#                             MAX_RETRY_COUNT, LONG_WAIT_TIME)
#                 time.sleep(LONG_WAIT_TIME)
#                 retry_count = 0 

#     CLIENT_READY = False
#     logger.info("🔌 Client thread terminated.")


# if __name__ == "__main__":
#     # Sensor Start
#     sensors = [
#         threading.Thread(target=listener_job, args=("Rain Gauge", dummy_data)),
#         threading.Thread(target=listener_job, args=("Flood Sensor", dummy_data)),

#         threading.Thread(target=counter_thread)
#     ]

#     for sensor in sensors:
#         sensor.start()

#     # Start Client on thread to do not block main
#     client_thread = threading.Thread(target=client)
#     client_thread.start()

#     try:
#         while True:
#             time.sleep(1) # Principal thread waits
#     except KeyboardInterrupt:
#         logger.info("🛑 Stopping all threads...")
#         STOP_EVENT.set()
#     finally:
#         for sensor in sensors:
#             sensor.join()

#         client_thread.join() # Wait until the client stop
#         logger.info("👋 All threads stopped")
#         sys.exit(0)

