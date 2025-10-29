"""
This program is created to test the multi-node connection
and the synchronization with the server
"""

import os
import sys
import time
import json
import socket
import threading
from datetime import datetime
from dotenv import load_dotenv


##############################################
from dummy_node import get_data as dummy_data
#from flood_sensor import get_data as flood_sensor_data
##############################################


# === ENVIRONMENT  VARIABLES ===
load_dotenv("../../Env/.env.config")  # Config env variables

# Use Localhost if run with an argument
RECEIVER_HOST =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))


def send_to_receiver(thread_name, data):
    payload = {
        "thread": thread_name,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"📡 Connecting to: {RECEIVER_HOST}:{RECEIVER_PORT}")
            s.connect((RECEIVER_HOST, RECEIVER_PORT))

            # 1️⃣ Esperar la señal del servidor
            handshake = s.recv(1024).decode("utf-8")
            if handshake != "READY":
                print(f"[{thread_name}] ⚠️ Servidor no listo, handshake falló: {handshake}")
                return
            print(f"[{thread_name}] ✅ Handshake OK")

            # 2️⃣ Enviar los datos
            s.sendall(json.dumps(payload).encode("utf-8"))

            # 3️⃣ Esperar confirmación final
            response = s.recv(1024).decode("utf-8")
            print(f"[{thread_name}] Respuesta del servidor: {response}")

    except Exception as e:
        print(f"[{thread_name}] ⚠️ Error enviando datos: {e}")


def listener_job(name, func):
    while True:
        data = func()
        print(f"[{name}] Datos generados: {data}")
        send_to_receiver(name, data)
        time.sleep(60)  # Espera 1 hora antes del siguiente envío


if __name__ == "__main__":
    t1 = threading.Thread(target=listener_job, args=("Sensor1", dummy_data))
    t2 = threading.Thread(target=listener_job, args=("Sensor2", dummy_data))

    t1.start()
    t2.start()

    t1.join()
    t2.join()
