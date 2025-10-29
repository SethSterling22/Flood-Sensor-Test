"""
This program deploys two threads to "flood_sensor.py" and 
"rain_gauge.py" to collect the various signals they send, 
while also synchronizing and packaging that information. 
It also connects to the server running "metrics_receiver.py," 
either locally or externally. "main.py" maintains the connection 
to the server and sends the previously packaged information 
from both sensors.
"""


#import signal
#import csv
import os
import sys
import time
import json
import socket
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv


##############################################################
# from rain_gauge import get_data as rain_gauge_data
from rain_gauge import get_rain_data as rain_gauge_data
#from flood_sensor import get_data as flood_sensor_data
from flood_sensor import get_flood_data as flood_sensor_data
##############################################################


# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables
LOG_DIR = "./Logs/"
# Create Directory
os.makedirs(LOG_DIR, exist_ok=True)


# Use Localhost if run.sh is executed as ExitNode
RECEIVER_HOST =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
NODE_ID = os.getenv("NODE_ID", "default_node")


# === LOGGING SETUP ===
# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'main.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
STOP_EVENT = threading.Event()



# === SEND INFORMATION TO THE SERVER ===
def send_to_receiver(thread_name, data):
    """Collect data and send it to the server"""

    # Packet structure
    payload = {
        "thread": thread_name,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10)  # Connection timeout
            
            logger.info(f"📡 Connecting to {RECEIVER_HOST}:{RECEIVER_PORT}")
            s.connect((RECEIVER_HOST, RECEIVER_PORT))

            # Step 1: Identification
            id_request = s.recv(1024)
            if id_request != b"NODE_ID_REQUEST":
                logger.error("⚠️ Protocol error: expected ID request")
                return

            s.sendall(NODE_ID.encode('utf-8'))
            response = s.recv(1024)
            
            if response != b"READY":
                logger.error(f"⚠️ Server not ready: {response.decode()}")
                return

            # Step 2: Send data
            s.sendall(json.dumps(payload).encode('utf-8'))
            final_response = s.recv(1024).decode('utf-8')
            
            if final_response == "OK_QUEUED":
                logger.info(f"✅ Data queued for processing ({thread_name})")
            else:
                logger.warning(f"⚠️ Server response: {final_response}")

    except socket.timeout:
        logger.error("⌛ Connection timeout")
    except Exception as e:
        logger.error(f"🔴 Connection error: {str(e)}")


# === THREADS MANAGER ===
def listener_job(thread_name, func):
    """Sync the data from threads"""
    while not STOP_EVENT.is_set():
        try:
            data = func()
            logger.info(f"[{thread_name}] Retrieved data: {data}")
            send_to_receiver(thread_name, data)
        except Exception as e:
            logger.error(f"[{thread_name}] Error: {str(e)}")
        
        STOP_EVENT.wait(60)  # Wait 60 seconds until STOP_EVENT


# === START THE PROGRAMS IN THREADS ===
if __name__ == "__main__":
    # Sensor Start
    sensors = [
        threading.Thread(target=listener_job, args=("🌧️ Rain Gauge", rain_gauge_data)),
        threading.Thread(target=listener_job, args=("💧 Flood Sensor", flood_sensor_data))
    ]

    for sensor in sensors:
        sensor.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Stopping all threads...")
        STOP_EVENT.set()
    finally:
        for sensor in sensors:
            sensor.join()
        logger.info("👋 All threads stopped")
        sys.exit(0)





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

