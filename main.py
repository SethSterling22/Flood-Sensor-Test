"""
Put something here
"""



import socket
import threading
#import signal
import sys
import time
import logging
#import csv
import os

# Packing Libraries 
import json
from datetime import datetime


from dotenv import load_dotenv



##############################################
from rain_gauge import get_data as rain_gaunge_data
# from rain_gauge import get_rain_data as rain_gaunge_data
#from flood_sensor import get_data as flood_sensor_data
from flood_sensor import get_flood_data as flood_sensor_data
##############################################


# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables
LOG_DIR = "./Logs/"
# PID_FILE = "./PID/"

# Use Localhost if run.sh is executed as ExitNode
RECEIVER_HOST =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))


# Create Directory
os.makedirs(LOG_DIR, exist_ok=True)

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR,'main.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



# === SEND INFORMATION TO THE SERVER ===
def send_to_receiver(thread_name, data):

    # Must be changed !!!
    payload = {
        "thread": thread_name,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"📡 Connecting to: {RECEIVER_HOST}:{RECEIVER_PORT}")
            logger.info(f"📡 Connecting to: {RECEIVER_HOST}:{RECEIVER_PORT}")

            s.connect((RECEIVER_HOST, RECEIVER_PORT))

            # Wait for the Connection Server response
            handshake = s.recv(1024).decode("utf-8")

            if handshake != "READY":
                print(f"[{thread_name}] ⚠️ Server not ready, handshake failed: {handshake}")
                logger.info(f"[{thread_name}] ⚠️ Server not ready, handshake failed: {handshake}")
                return
            print(f"[{thread_name}] ✅ Handshake OK")


            # Send data
            s.sendall(json.dumps(payload).encode("utf-8"))

            # Waiting for the acknowledgement
            response = s.recv(1024).decode("utf-8")
            print(f"[{thread_name}] Server response: {response}")
            logger.info(f"[{thread_name}] Server response: {response}")
            
    except Exception as e:
        # Esto asegura que el mensaje de error se registre inmediatamente
        print(f"[{thread_name}] ⚠️ Error sending data: {e}")
        logger.error(f"[{thread_name}] ⚠️ Error sending data: {e}")


# def listener_job(name, func):
#     while True:
#         data = func()
#         print(f"[{name}] Generated data: {data}")
#         send_to_receiver(name, data)
#         time.sleep(60)  # Sleep for 1 minute


# === Cierre Cooperativo de Hilos ===
STOP_EVENT = threading.Event()


def listener_job(thread_name, func):
    # Cambiamos while True a while not STOP_EVENT.is_set()
    while not STOP_EVENT.is_set():
        data = func()
        print(f"[{thread_name}] Generated data: {data}")

        if thread_name == "Flood Sensor":
            if data == "Detected":
                logger.info("Flooding has been DETECTED and sent to submmit the Model!")
            elif data == "No Detected":
                logger.info("No flooding detected")


        send_to_receiver(thread_name, data)
        
        # Usamos wait() en lugar de sleep() para que el hilo pueda ser interrumpido
        # El hilo espera 60 segundos, pero si STOP_EVENT se activa, espera se rompe inmediatamente.
        STOP_EVENT.wait(60)


# # === START THE PROGRAMS IN THREADS ===
if __name__ == "__main__":
    t1 = threading.Thread(target=listener_job, args=("Rain Gaunge", rain_gaunge_data))
    t2 = threading.Thread(target=listener_job, args=("Flood Sensor", flood_sensor_data))

    t1.start()
    t2.start()


    try:
        # Espera en un bucle ligero para que la interrupción de teclado funcione bien
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Stopping all threads...")
        # 1. Levanta la bandera: Esto rompe los bucles while de listener_job
        STOP_EVENT.set() 
        
    finally:
        # 2. Espera a que los hilos terminen (join)
        t1.join()
        t2.join()

        # 3. GARANTÍA FINAL: Llama a shutdown después de que los hilos mueren
        logging.shutdown() 
        logger.info("All threads stopped. Program exit.")
        sys.exit(0)

    # t1.join()
    # t2.join()

