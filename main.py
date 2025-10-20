import socket
import threading
import signal
import sys
import time
import logging
import csv


# Packing Libraries 
import json
from datetime import datetime


from dotenv import load_dotenv
import os



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
port_env = os.getenv("RECEIVER_PORT", "4040")
try:
    RECEIVER_PORT = int(port_env)
except ValueError:
    RECEIVER_PORT = 4040  # fallback



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

    except Exception as e:
        logging.shutdown() 
        print(f"[{thread_name}] ⚠️ Error sending data: {e}")

    finally:
        # Save the Logger buffer
        logging.shutdown()
        logger.info("Cleanup handled by gpiozero.") 


def listener_job(name, func):
    while True:
        data = func()
        print(f"[{name}] Generated data: {data}")
        send_to_receiver(name, data)
        time.sleep(60)  # Sleep for 1 minute


# # === START THE PROGRAMS IN THREADS ===
if __name__ == "__main__":
    t1 = threading.Thread(target=listener_job, args=("Rain Gaunge", rain_gaunge_data))
    t2 = threading.Thread(target=listener_job, args=("Flood Sensor", flood_sensor_data))

    t1.start()
    t2.start()

    t1.join()
    t2.join()
