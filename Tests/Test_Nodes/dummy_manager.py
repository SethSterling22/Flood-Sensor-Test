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
NODE_ID = "NODE_Dummy2"  # Must start with "NODE_"
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
            
            # if CLIENT_READY:
            #     with BUFFER_LOCK:
            #         SENSOR_DATA_BUFFER.append(data)
            #         print("Check packets. ")
            #         print(SENSOR_DATA_BUFFER)
            # Start the Threads and wait
            #data = func()

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
#                                 payload_str = json.dumps(data_to_send).encode('utf-8')
#                                 payload_length = str(len(payload_str)).zfill(8).encode('utf-8')
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
                    raise socket.error(f"Server sent unexpected initial response: '{response}'")

                # Restart counter if is CONNECTED
                retry_count = 0

                # 2. Send the NODE_ID to index in the Server
                s.sendall(NODE_ID.encode('utf-8'))
                response = s.recv(1024).decode().strip()
                logger.info("📡 SERVER respond with: %s", response)

                
                if response != "ID_RECEIVED":
                    logger.error("⚠️ NODE ID not indexed: %s", response)
                    raise socket.error(f"NODE ID not indexed. Server response: '{response}'")

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
        threading.Thread(target=listener_job, args=("Rain Gauge", dummy_data)),
        threading.Thread(target=listener_job, args=("Flood Sensor", dummy_data))
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


