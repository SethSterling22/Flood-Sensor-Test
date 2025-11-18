"""
This program deploys two threads to "flood_sensor.py" and 
"rain_gauge.py" to collect the various signals they send, 
while also synchronizing and packaging that information. 
It also connects to the server running "metrics_receiver.py," 
either locally or externally. "main.py" maintains the connection 
to the server and sends the previously packaged information 
from both sensors.
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
##############################################################
from Sensors.rain_gauge import get_rain_data as rain_gauge_data
from Sensors.flood_sensor import get_flood_data as flood_sensor_data
from Sensors.temp_and_humid_sensor import get_temp_and_humid_data as temp_humid_data
##############################################################



# === ENVIRONMENT VARIABLES ===
load_dotenv("./Env/.env.config")
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)


# === CONNETION SETTINGS ===
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
#NODE_ID = "NODE_Dummy1"  # Must start with "NODE_"
NODE_ID = f"NODE_{os.getenv('NODE_ID', 'default')}"


# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'main_client.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# === GLOBAL VARIABLES ===
BUFFER_LOCK = threading.Lock()
SENSOR_DATA_BUFFER = [] 
CLIENT_READY = False
STOP_EVENT = threading.Event()



# --- Thread Sensor Funcioon ---
def listener_job(sensor_name, func):
    """
    Manages sensor data collection and 
    append it to the BUFFER.
    """

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
                    now = datetime.datetime.now()
                    time_string = f"{now.hour}:{now.minute}:{now.second}"
                    SENSOR_DATA_BUFFER.append({
                        'sensor': sensor_name,
                        'timestamp': time_string,
                        'value': data
                    })
                    logger.debug("Buffered %s data: %.2f", sensor_name, data)
                # Wait for 60 seconds (the collection interval)
                STOP_EVENT.wait(60)
            # Wait until next measurement interval
            # time.sleep(POLL_INTERVAL)
                
        except Exception as e:
            logger.error("%s thread error: %s", sensor_name, str(e))
            time.sleep(5)  # Wait before retrying


# def client():
#     """
#     Manages the connection and the messages from the server.
#     """

#     global CLIENT_READY

#     # 1. Try connection to server
#     try:
#         # "With" statements makes socket close automatically
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(300) 
#             logger.info("📡 Connecting to %s:%d", RECEIVER_HOST, RECEIVER_PORT)
#             s.connect((RECEIVER_HOST, RECEIVER_PORT))

#             #while True:
#             # Wait until server connects and send the CONNECTED message
#             response = s.recv(1024).decode().strip()
#             logger.info("📡 SERVER response on Connection: %s", response)

#             # Check the connection message
#             if response != "CONNECTED":
#                 logger.error("⚠️ Error while connecting on server: %s", response)
#                 return

#             # 2. Send the NODE_ID to index in the Server
#             s.sendall(NODE_ID.encode('utf-8'))
#             response = s.recv(1024).decode().strip()
#             logger.info("📡 SERVER respond with: %s", response)

            
#             if response != "ID_RECEIVED":
#                 logger.error("⚠️ NODE ID not indexed: %s", response)
#                 return

#             # --- CONNECTION STABLISHED AND NODE REGISTERED ---
#             # Allows the threads data recording 
#             CLIENT_READY = True
#             logger.info("✅ Connection established and ID registered. Starting data collection... 📊")

#             # 3. Principal receiver loop and data sending
#             while not STOP_EVENT.is_set():
#                 try:
#                     # Implicit 300s (5min) Timeout
#                     # Waits one second to check STOP_EVENT
#                     s.settimeout(1)

#                     # Wait one minute for the READY_TO_INDEX from the server
#                     try:
#                         message = s.recv(1024).decode().strip()
#                     except socket.timeout:
#                         continue

#                     # If server is ready to index (A minute from the connection already happened and it's Synchronized):
#                     if message == "READY_TO_INDEX":
#                         logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")

#                         # Get and clean BUFFERED data
#                         with BUFFER_LOCK:
#                             data_to_send = SENSOR_DATA_BUFFER.copy()
#                             SENSOR_DATA_BUFFER.clear()

#                         if data_to_send:
#                             try:
#                                 #payload_str = json.dumps(data_to_send)
#                                 payload_str = json.dumps(data_to_send)
#                                 payload_length = str(len(payload_str)).zfill(8)
#                                 logger.info("📤 Sending %s data points.", len(data_to_send))
#                                 # s.sendall(payload_length) 
#                                 logger.info("DATA sent:\n %s", data_to_send)
#                                 # s.sendall(payload)
#                             except TypeError as e:
#                                 logger.error("⚠️ Error serializing JSON. Check data format: %s", e)
#                                 return # Fallo crítico, cerrar conexión
                            
#                         else:
#                             # 2. ESCENARIO 'NO_DATA'
#                             logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
#                             payload_str = "NO_DATA"





#                         # 3. CODIFICACIÓN Y PREPARACIÓN DEL PROTOCOLO DE LONGITUD (Común para JSON y NO_DATA)
#                         payload_bytes = payload_str.encode('utf-8')
#                         payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')
                        
#                         # 4. ENVÍO (Longitud + Payload)
#                         s.sendall(payload_length_bytes) # Envía 8 bytes (ej: b'00000045')
#                         # logger.info("DATA sent:\n %s", data_to_send) # logging the list before encoding
#                         s.sendall(payload_bytes) # Envía la data

#                         # 5. Esperando la confirmación del servidor
#                         s.settimeout(30) # Aumentar temporalmente el timeout
#                         ack = s.recv(1024).decode().strip()
#                         s.settimeout(1) # Volver al timeout corto

#                         if ack == "DATA_RECEIVED":
#                             logger.info("👍 Data successfully indexed by server.")
#                         elif ack == "JSON_ERROR":
#                             # 🌟 NUEVO HANDLER: El servidor reportó un error de decodificación
#                             logger.error("❌ Servidor falló al decodificar la data JSON. La data no fue guardada.")
#                         else:
#                             logger.error("❌ Server ACK error: %s", ack)



#                         #     # Waiting for the server confirmation
#                         #     s.settimeout(30) # Increase temporally the timeout time
#                         #     ack = s.recv(1024).decode().strip()
#                         #     s.settimeout(1) # Set short timeout again

#                         #     if ack == "DATA_RECEIVED":
#                         #         logger.info("👍 Data successfully indexed by server.")
#                         #     else:
#                         #         logger.error("❌ Server ACK error: %s", ack)
#                         # else:
#                         #     logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
#                         #     s.sendall("NO_DATA") # Send NO_DATA if BUFFER is empty

                            

#                     elif message:
#                         logger.warning("Received unknown message: %s", message)

#                 except ConnectionResetError:
#                     logger.error("🚫 Connection lost (Server closed the connection).")
#                     break
#                 except Exception as e:
#                     logger.error("🔌 Fatal error during communication: %s", e)
#                     break

#     # Catch errors
#     except socket.error as e:
#         logger.error("❌ Failed to connect to server: %s", e)
#     finally:
#         CLIENT_READY = False
#         logger.info("🔌 Client socket closed.")



def client():
    """
    Manages the connection and the messages from the server.
    """

    MAX_RETRY_COUNT = 3
    SHORT_WAIT_TIME = 10  # 10 segundos
    LONG_WAIT_TIME = 300  # 5 minutos
    retry_count = 0

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
                response = s.recv(1024).decode().strip()
                logger.info("📡 SERVER response on Connection: %s", response)

                # Check the connection message
                if response != "CONNECTED":
                    logger.error("⚠️ Error while connecting on server: %s", response)
                    return

                # Restart counter if is CONNECTED
                retry_count = 0

                # 2. Send the NODE_ID to index in the Server
                s.sendall(NODE_ID.encode('utf-8'))
                response = s.recv(1024).decode().strip()
                logger.info("📡 SERVER respond with: %s", response)

                
                if response != "ID_RECEIVED":
                    logger.error("⚠️ NODE ID not indexed: %s", response)
                    return

                # --- CONNECTION STABLISHED AND NODE REGISTERED ---
                # Allows the threads data recording 
                CLIENT_READY = True
                logger.info("✅ Connection established and ID registered. Starting data collection... 📊")
                
                # 3. Principal receiver loop and data sending
                while not STOP_EVENT.is_set():
                    try:
                        # Waits one second to check STOP_EVENT
                        s.settimeout(1)

                        # Wait one minute for the READY_TO_INDEX from the server
                        try:
                            message = s.recv(1024).decode().strip()
                        except socket.timeout:
                            continue

                        # If server is ready to index:
                        if message == "READY_TO_INDEX":
                            logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")
                            
                            # Get and clean BUFFERED data
                            with BUFFER_LOCK:
                                data_to_send = SENSOR_DATA_BUFFER.copy()
                                

                            # --- CONSTRUCCIÓN DEL PAYLOAD ---
                            if data_to_send:
                                # 1. Serializar data estructurada a CADENA JSON (str)
                                try:
                                    payload_str = json.dumps(data_to_send)
                                    logger.info("📤 Sending %s data points.", len(data_to_send))
                                    logger.info("DATA sent:\n %s", data_to_send)
                                except TypeError as e:
                                    logger.error("⚠️ Error serializing JSON. Check data format: %s", e)
                                    return # Fallo crítico, cerrar conexión
                                
                            else:
                                # 2. ESCENARIO 'NO_DATA' (str)
                                logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
                                payload_str = "NO_DATA"

                            # 3. CODIFICACIÓN Y PREPARACIÓN DEL PROTOCOLO DE LONGITUD (Común para todos los casos)
                            
                            # Convertir el contenido (str) a BYTES
                            payload_bytes = payload_str.encode('utf-8')
                            
                            # Calcular la longitud de los bytes y codificar el prefijo de 8 bytes
                            payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')
                            
                            # 4. ENVÍO (Longitud + Payload)
                            s.sendall(payload_length_bytes) # Envía 8 bytes (prefijo)
                            s.sendall(payload_bytes)        # Envía la data

                            # 5. Esperando la confirmación del servidor
                            s.settimeout(30) # Aumentar temporalmente el timeout
                            ack = s.recv(1024).decode().strip()
                            s.settimeout(1) # Volver al timeout corto

                            if ack == "DATA_RECEIVED":
                                logger.info("👍 Data successfully indexed by server.")
                                # Just clean the buffer if the Data was received
                                with BUFFER_LOCK:
                                    SENSOR_DATA_BUFFER.clear()

                            elif ack == "JSON_ERROR":
                                # 🌟 HANDLER: El servidor reportó un error de decodificación
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
            CLIENT_READY = False # Asegura que la recopilación de datos se detenga si se pierde la conexión
            
            # 🌟 Lógica de espera escalonada
            retry_count += 1
            
            if retry_count <= MAX_RETRY_COUNT:
                logger.info("⏳ Waiting %d seconds before next retry...", SHORT_WAIT_TIME)
                time.sleep(SHORT_WAIT_TIME)
            else:
                logger.info("😴 Failed %d times. Waiting %d seconds (5 minutes) before resetting attempts...", 
                            MAX_RETRY_COUNT, LONG_WAIT_TIME)
                time.sleep(LONG_WAIT_TIME)
                retry_count = 0 # Resetear el contador después de la espera larga

        # Catch errors
        except socket.error as e:
            logger.error("❌ Failed to connect to server: %s", e)
        finally:
            CLIENT_READY = False
            logger.info("🔌 Client socket closed.")

    # Thread closed
    logger.info("🔌 Client thread terminated.")



if __name__ == "__main__":
    """
    Work with thread synchronization, start and
    end the program.
    """

    # Sensor Start
    sensors = [
        threading.Thread(target=listener_job, args=("Rain Gauge", rain_gauge_data)),
        threading.Thread(target=listener_job, args=("Flood Sensor", flood_sensor_data)),
        threading.Thread(target=listener_job, args=("Temperature and Humidity", temp_humid_data))
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


#import signal
#import csv
# import os
# import sys
# import time
# import json
# import socket
# import logging
# import threading
# from datetime import datetime
# from dotenv import load_dotenv
# from typing import Dict, Any, List


# ##############################################################
# # from rain_gauge import get_data as rain_gauge_data
# from rain_gauge import get_rain_data as rain_gauge_data
# #from flood_sensor import get_data as flood_sensor_data
# from flood_sensor import get_flood_data as flood_sensor_data
# ##############################################################


# # === ENVIRONMENT  VARIABLES ===
# load_dotenv("./Env/.env.config")  # Config env variables
# LOG_DIR = "./Logs/"
# # Create Directory
# os.makedirs(LOG_DIR, exist_ok=True)


# # Use Localhost if run.sh is executed as ExitNode
# RECEIVER_HOST =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
# RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
# NODE_ID = f"NODE_{os.getenv('NODE_PREFIX', 'default')}"  # Ensure NODE_ prefix


# # === LOGGING SETUP ===
# # Logging
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format='%(asctime)s - %(levelname)s - %(message)s',
# #     handlers=[
# #         logging.FileHandler(os.path.join(LOG_DIR, 'main.log')),
# #         logging.StreamHandler()
# #     ]
# # )
# # logger = logging.getLogger(__name__)
# # STOP_EVENT = threading.Event()



# # === GLOBAL DATA AND SYNCHRONIZATION ===
# DATA_BUFFER: Dict[str, Any] = {}
# DATA_LOCK = threading.Lock()
# STOP_EVENT = threading.Event()
# INITIALIZED_EVENT = threading.Event() # New: Signal that connection is established

# # === LOGGING SETUP ===
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(os.path.join(LOG_DIR, 'main.log'), encoding='utf-8'),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)




# def send_data_payload(s: socket.socket) -> bool:
#     """Sends the consolidated payload from the buffer."""
    
#     global DATA_BUFFER
    
#     with DATA_LOCK:
#         # Check if both sensor readings are present
#         if len(DATA_BUFFER) < 2:
#             logger.warning("Buffer not complete. Skipping send for this interval.")
#             return False

#         payload = {
#             "node_id": NODE_ID,
#             "timestamp": datetime.now().isoformat(),
#             "metrics": DATA_BUFFER
#         }
        
#         try:
#             # Send data
#             s.sendall(json.dumps(payload).encode('utf-8'))
            
#             # Receive acknowledgment
#             s.settimeout(5) # Shorter timeout for response
#             response = s.recv(1024).decode('utf-8')
#             s.settimeout(None) # Reset timeout
            
#             if response == "OK_QUEUED":
#                 logger.info(f"✅ Consolidated payload sent successfully.")
#                 DATA_BUFFER = {} # Clear buffer after successful send
#                 return True
#             else:
#                 logger.warning(f"⚠️ Server response: {response}")
#                 return False

#         except socket.timeout:
#             logger.error("⌛ Timeout waiting for server acknowledgment.")
#         except Exception as e:
#             logger.error(f"🔴 Error during data transmission: {str(e)}")
            
#         return False


# def manage_connection(host: str, port: int):
#     """Manages the persistent connection lifecycle."""
    
#     while not STOP_EVENT.is_set():
#         try:
#             logger.info(f"📡 Attempting connection to {host}:{port}")
#             s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             s.connect((host, port))
#             s.settimeout(None) # Blocking mode for persistent connection
            
#             # --- Initial Handshake ---
            
#             # 1. Receive ID Request
#             s.settimeout(10)
#             id_request = s.recv(1024)
#             if id_request != b"NODE_ID_REQUEST":
#                 logger.error("❌ Protocol error: expected ID request.")
#                 s.close()
#                 raise ConnectionError("Protocol mismatch.")

#             # 2. Send Node ID
#             s.sendall(NODE_ID.encode('utf-8'))
#             response = s.recv(1024)
            
#             if response != b"READY":
#                 logger.error(f"❌ Server rejected ID: {response.decode()}")
#                 s.close()
#                 raise ConnectionError("Server ID rejection.")

#             logger.info("🟢 Connection established and READY signal received. Starting data collection.")
#             INITIALIZED_EVENT.set() # Signal sensor threads to start collecting
            
#             # --- Persistent Sending Loop ---
            
#             while not STOP_EVENT.is_set():
#                 # The sender_job now handles the timed send
#                 # This thread remains alive just to hold the connection open.
                
#                 # Check for server-initiated disconnect (e.g., timeout from server)
#                 try:
#                     s.settimeout(1) # Check for data once per second
#                     # If we receive data here, it means the server is sending a command (e.g., disconnect)
#                     data = s.recv(1024)
#                     if data:
#                         logger.warning(f"Server sent unexpected data: {data.decode()}. Disconnecting.")
#                         s.close()
#                         raise ConnectionResetError
#                 except socket.timeout:
#                     # Expected timeout, connection is stable
#                     pass
                
#                 time.sleep(1) # Minor delay to prevent excessive CPU usage

#         except (ConnectionRefusedError, ConnectionError, socket.error) as e:
#             logger.error(f"🔌 Connection lost or refused: {str(e)}. Retrying in 10s...")
#             INITIALIZED_EVENT.clear() # Stop data collection during downtime
#             STOP_EVENT.wait(10) # Wait before retrying
        
#         except Exception as e:
#             logger.error(f"🔴 Unexpected critical error in connection manager: {str(e)}. Retrying in 10s...")
#             INITIALIZED_EVENT.clear()
#             STOP_EVENT.wait(10)


# def sensor_job(thread_name: str, func: callable):
#     """
#     Collects data from a sensor and adds it to the shared buffer.
#     """
    
#     logger.info(f"[{thread_name}] Waiting for server connection READY signal...")
    
#     # Wait until the connection manager receives 'READY' from the server
#     INITIALIZED_EVENT.wait()
    
#     while not STOP_EVENT.is_set():
#         try:
#             data = func()
#             logger.info(f"[{thread_name}] Collected data: {data}")
            
#             with DATA_LOCK:
#                 DATA_BUFFER[thread_name] = data
                
#             # Wait for 60 seconds (the collection interval)
#             STOP_EVENT.wait(60)
            
#         except Exception as e:
#             logger.error(f"[{thread_name}] Critical error during data collection: {str(e)}")
#             STOP_EVENT.wait(60) # Wait before next attempt


# def sender_job(host: str, port: int):
#     """
#     Manages the timed synchronization and data sending every 60 seconds.
#     """
#     # Wait until the connection is established before starting the send cycle
#     INITIALIZED_EVENT.wait()
    
#     s = None
#     while not STOP_EVENT.is_set():
#         # Synchronize to the next minute boundary (optional, for precision)
#         # time_to_wait = 60 - (time.time() % 60)
#         # STOP_EVENT.wait(time_to_wait)
        
#         STOP_EVENT.wait(60) # Wait 60 seconds for synchronization

#         try:
#             # We need to get the socket object from the persistent connection manager
#             # Since the connection manager holds the socket, we'll try to find it via the global state.
#             # A simpler way for a single connection is to pass the socket reference, but for the refactor:
            
#             # This is simplified: in a real-world scenario, the manage_connection thread would expose 
#             # the active socket reference securely. For this problem, we'll assume the socket is 
#             # accessible or we will create a *temporary* connection (which defeats the "persistent" requirement).
#             # Sticking to the requirement, we need the active socket from manage_connection.
            
#             # Since the current structure makes passing the persistent socket difficult, 
#             # we will merge the sending logic into the connection manager for safety.
#             # However, to maintain two separate threads (sender_job and manage_connection):
            
#             # A HACK FOR SIMPLICITY: Re-implement the persistent socket creation here, 
#             # which is less clean but fulfills the timed sending requirement.
            
#             # --- Forcing Persistent Socket Access (A bit messy but works for this structure) ---
#             s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             s.connect((host, port))
            
#             # Re-perform the READY handshake to get the server to accept the data
#             s.sendall(NODE_ID.encode('utf-8'))
#             s.recv(1024) # Expecting READY/OK
            
#             send_data_payload(s)
#             s.close()
            
#         except Exception as e:
#             logger.error(f"🔴 Sender Job failed to send data: {str(e)}")
            
#         finally:
#             if s:
#                 s.close()


# if __name__ == "__main__":
    
#     # Start the connection manager thread (persistent connection)
#     connection_thread = threading.Thread(target=manage_connection, args=(RECEIVER_HOST, RECEIVER_PORT))
#     connection_thread.start()
    
#     # Wait for the connection to be established before starting sensor/sender threads
#     # INITIALIZED_EVENT.wait() 
    
#     # Start sensor threads (collecting data)
#     sensor_threads = [
#         threading.Thread(target=sensor_job, args=("🌧️ Rain Gauge", rain_gauge_data)),
#         threading.Thread(target=sensor_job, args=("💧 Flood Sensor", flood_sensor_data))
#     ]

#     for t in sensor_threads:
#         t.start()
        
#     # Start the dedicated sender thread (timed sending)
#     sender_thread = threading.Thread(target=sender_job, args=(RECEIVER_HOST, RECEIVER_PORT))
#     sender_thread.start()


#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("🛑 Stopping all threads...")
#         STOP_EVENT.set()
        
#         connection_thread.join()
#         sender_thread.join()
#         for t in sensor_threads:
#             t.join()
            
#         logger.info("👋 All threads stopped. Exiting.")
#         sys.exit(0)









# def send_to_receiver(thread_name, data):
#     """Send sensor data to the receiver server"""
#     payload = {
#         "thread": thread_name,
#         "timestamp": datetime.now().isoformat(),
#         "data": data
#     }

#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(10)
            
#             logger.info(f"📡 Connecting to server at {RECEIVER_HOST}:{RECEIVER_PORT}")
#             s.connect((RECEIVER_HOST, RECEIVER_PORT))

#             # Node identification
#             id_request = s.recv(1024)
#             if id_request != b"NODE_ID_REQUEST":
#                 logger.error("❌ Protocol error: expected ID request")
#                 return False

#             s.sendall(NODE_ID.encode('utf-8'))
#             response = s.recv(1024)
            
#             if response != b"READY":
#                 logger.error(f"❌ Server not ready: {response.decode()}")
#                 return False

#             # Send data
#             s.sendall(json.dumps(payload).encode('utf-8'))
#             final_response = s.recv(1024).decode('utf-8')
            
#             if final_response == "OK_QUEUED":
#                 logger.info(f"✅ Data queued at server ({thread_name})")
#                 return True
#             else:
#                 logger.warning(f"⚠️ Server response: {final_response}")
#                 return False

#     except socket.timeout:
#         logger.error("⌛ Connection timeout with server")
#     except ConnectionRefusedError:
#         logger.error("🔌 Connection refused - server may be down")
#     except Exception as e:
#         logger.error(f"🔴 Unexpected error: {str(e)}")
#     return False


# def sensor_job(thread_name, func):
#     """
#     Thread worker for each sensor
#     """
    
#     retry_count = 0
#     max_retries = 3
    
#     while not STOP_EVENT.is_set():
#         try:
#             data = func()
#             logger.info(f"[{thread_name}] Collected data: {data}")
            
#             success = send_to_receiver(thread_name, data)
            
#             if not success and retry_count < max_retries:
#                 retry_count += 1
#                 logger.warning(f"🔁 [{thread_name}] Retry {retry_count}/{max_retries} in 30s...")
#                 STOP_EVENT.wait(30)  # Short wait for retry
#                 continue
                
#             retry_count = 0
#             STOP_EVENT.wait(120)  # Normal 60-second interval
            
#         except Exception as e:
#             logger.error(f"❗❗ [{thread_name}] Critical error: {str(e)}")
#             STOP_EVENT.wait(120)  # Wait before next attempt


# if __name__ == "__main__":
#     # Start sensor threads
#     sensors = [
#         threading.Thread(target=sensor_job, args=("🌧️ Rain Gauge", rain_gauge_data)),
#         threading.Thread(target=sensor_job, args=("💧 Flood Sensor", flood_sensor_data))
#     ]

#     for sensor in sensors:
#         sensor.start()

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("🛑 Stopping all sensors...")
#         STOP_EVENT.set()
#         for sensor in sensors:
#             sensor.join()
#         logger.info("👋 All sensors stopped")
#         sys.exit(0)








# === SEND INFORMATION TO THE SERVER ===
# def send_to_receiver(thread_name, data):
#     """Collect data and send it to the server"""

#     # Packet structure
#     payload = {
#         "thread": thread_name,
#         "timestamp": datetime.now().isoformat(),
#         "data": data
#     }

#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(10)  # Connection timeout
            
#             logger.info(f"📡 Connecting to {RECEIVER_HOST}:{RECEIVER_PORT}")
#             s.connect((RECEIVER_HOST, RECEIVER_PORT))

#             # Step 1: Identification
#             id_request = s.recv(1024)
#             if id_request != b"NODE_ID_REQUEST":
#                 logger.error("⚠️ Protocol error: expected ID request")
#                 return

#             s.sendall(NODE_ID.encode('utf-8'))
#             response = s.recv(1024)
            
#             if response != b"READY":
#                 logger.error(f"⚠️ Server not ready: {response.decode()}")
#                 return

#             # Step 2: Send data
#             s.sendall(json.dumps(payload).encode('utf-8'))
#             final_response = s.recv(1024).decode('utf-8')
            
#             if final_response == "OK_QUEUED":
#                 logger.info(f"✅ Data queued for processing ({thread_name})")
#             else:
#                 logger.warning(f"⚠️ Server response: {final_response}")

#     except socket.timeout:
#         logger.error("⌛ Connection timeout")
#     except Exception as e:
#         logger.error(f"🔴 Connection error: {str(e)}")


# # === THREADS MANAGER ===
# def listener_job(thread_name, func):
#     """Sync the data from threads"""
#     while not STOP_EVENT.is_set():
#         try:
#             data = func()
#             logger.info(f"[{thread_name}] Retrieved data: {data}")
#             send_to_receiver(thread_name, data)
#         except Exception as e:
#             logger.error(f"[{thread_name}] Error: {str(e)}")
        
#         STOP_EVENT.wait(60)  # Wait 60 seconds until STOP_EVENT


# # === START THE PROGRAMS IN THREADS ===
# if __name__ == "__main__":
#     # Sensor Start
#     sensors = [
#         threading.Thread(target=listener_job, args=("🌧️ Rain Gauge", rain_gauge_data)),
#         threading.Thread(target=listener_job, args=("💧 Flood Sensor", flood_sensor_data))
#     ]

#     for sensor in sensors:
#         sensor.start()

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("🛑 Stopping all threads...")
#         STOP_EVENT.set()
#     finally:
#         for sensor in sensors:
#             sensor.join()
#         logger.info("👋 All threads stopped")
#         sys.exit(0)


###########################################################################################



# # === SEND INFORMATION TO THE SERVER ===
# def send_to_receiver(thread_name, data):

#     # Must be changed !!!
#     payload = {
#         "thread": thread_name,
#         "timestamp": datetime.now().isoformat(),
#         "data": data
#     }

#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             print(f"📡 Connecting to: {RECEIVER_HOST}:{RECEIVER_PORT}")
#             logger.info(f"📡 Connecting to: {RECEIVER_HOST}:{RECEIVER_PORT}")

#             s.connect((RECEIVER_HOST, RECEIVER_PORT))

#             # Wait for the Connection Server response
#             handshake = s.recv(1024).decode("utf-8")

#             if handshake != "READY":
#                 print(f"[{thread_name}] ⚠️ Server not ready, handshake failed: {handshake}")
#                 logger.info(f"[{thread_name}] ⚠️ Server not ready, handshake failed: {handshake}")
#                 return
#             print(f"[{thread_name}] ✅ Handshake OK")


#             # Send data
#             s.sendall(json.dumps(payload).encode("utf-8"))

#             # Waiting for the acknowledgement
#             response = s.recv(1024).decode("utf-8")
#             print(f"[{thread_name}] Server response: {response}")
#             logger.info(f"[{thread_name}] Server response: {response}")
            
#     except Exception as e:
#         # Esto asegura que el mensaje de error se registre inmediatamente
#         print(f"[{thread_name}] ⚠️ Error sending data: {e}")
#         logger.error(f"[{thread_name}] ⚠️ Error sending data: {e}")


# # def listener_job(name, func):
# #     while True:
# #         data = func()
# #         print(f"[{name}] Generated data: {data}")
# #         send_to_receiver(name, data)
# #         time.sleep(60)  # Sleep for 1 minute


# # === Cierre Cooperativo de Hilos ===
# STOP_EVENT = threading.Event()


# def listener_job(thread_name, func):
#     # Cambiamos while True a while not STOP_EVENT.is_set()
#     while not STOP_EVENT.is_set():
#         data = func()
#         print(f"[{thread_name}] Retrieved data: {data}")
#         logger.info(f"[{thread_name}] Retrieved data: {data}")

#         # if thread_name == "Flood Sensor":
#         #     if data == "Detected":
#         #         logger.info("Flooding has been DETECTED and sent to submmit the Model!")
#         #     elif data == "Not Detected":
#         #         logger.info("No flooding detected")


#         send_to_receiver(thread_name, data)
        
#         # Make all the signals wait for 1 minute to be sent again
#         STOP_EVENT.wait(60)


# # # === START THE PROGRAMS IN THREADS ===
# if __name__ == "__main__":
#     t1 = threading.Thread(target=listener_job, args=("🌧️ Rain Gauge", rain_gauge_data))
#     t2 = threading.Thread(target=listener_job, args=("💧 Flood Sensor", flood_sensor_data))

#     t1.start()
#     t2.start()


#     try:
#         # Espera en un bucle ligero para que la interrupción de teclado funcione bien
#         while True:
#             time.sleep(1)

#     except KeyboardInterrupt:
#         logger.info("Stopping all threads...")
#         # 1. Levanta la bandera: Esto rompe los bucles while de listener_job
#         STOP_EVENT.set() 
        
#     finally:
#         # 2. Espera a que los hilos terminen (join)
#         t1.join()
#         t2.join()

#         # 3. GARANTÍA FINAL: Llama a shutdown después de que los hilos mueren
#         logging.shutdown() 
#         logger.info("All threads stopped. Program exit.")
#         sys.exit(0)

#     # t1.join()
#     # t2.join()

