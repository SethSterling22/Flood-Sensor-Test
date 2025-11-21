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



def client():
    """
    Manages the connection and the messages from the server.
    """

    MAX_RETRY_COUNT = 3
    SHORT_WAIT_TIME = 15  # 15 segundos
    LONG_WAIT_TIME = 300  # 5 minutos
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
                        if message.startswith("READY_TO_INDEX") and SENSOR_DATA_BUFFER:
                            logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")

                            # Get and clean BUFFERED data
                            with BUFFER_LOCK:
                                
                                # Just send the data if the BUFFER is not empty
                                if not SENSOR_DATA_BUFFER:
                                    logger.info("🚫 Ignoring READY_TO_INDEX: Data buffer is empty.")
                                    continue

                                # Copy the buffer in a tmp variable
                                data_to_send = SENSOR_DATA_BUFFER.copy()

                            # PAYLOAD BUILDING 
                            if data_to_send:
                                # 1. Serialize structured data to JSON format
                                try:
                                    payload_str = json.dumps(data_to_send)
                                    logger.info("📤 Sending %s data points.", len(data_to_send))
                                    logger.info("DATA sent:\n %s", data_to_send)
                                except TypeError as e:
                                    logger.error("⚠️ Error serializing JSON. Check data format: %s", e)
                                    break 

                            else:
                                # 2. 'NO_DATA' escenario (str)
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

                            if ack == "DATA_RECEIVED" or ack.startswith("DATA_RECEIVED"):
                                
                                # If it is concatenated, send the warning
                                if ack.startswith("DATA_RECEIVED") and ack != "DATA_RECEIVED":
                                    logger.warning("⚠️ Received concatenated ACK: %s", ack)
                                else:
                                    logger.info("👍 Data successfully indexed by server.")
                                
                                # Clear buffer
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

