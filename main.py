import socket
import threading
import signal
import sys
import time


# Packing Libraries 
import json
from datetime import datetime


from dotenv import load_dotenv
import os



##############################################
from rain_gauge import get_data as rain_gaunge_data
from flood_sensor import get_data as flood_sensor_data
##############################################


# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables

# Use Localhost if run.sh is executed as ExitNode
RECEIVER_HOST =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
port_env = os.getenv("RECEIVER_PORT", "4040")
try:
    RECEIVER_PORT = int(port_env)
except ValueError:
    RECEIVER_PORT = 4040  # fallback






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
    t1 = threading.Thread(target=listener_job, args=("Rain Gaunge", rain_gaunge_data))
    t2 = threading.Thread(target=listener_job, args=("Flood Sensor", flood_sensor_data))

    t1.start()
    t2.start()

    t1.join()
    t2.join()


# def run_listener1():
#     generate_signal1()

# def run_listener2():
#     generate_signal2()




# # === START THE PROGRAM IN THREADS ===
# def start_weather_services():


#     time.sleep(60)  # Espera una hora
#     print("Sincronizando con el receptor...")
#     # Aquí iría la lógica para enviar los datos al receiver.py



#     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     client_socket.connect(("127.0.0.1", 12345))

#     while True:
#         # Recibir mensajes del servidor
#         response = client_socket.recv(1024).decode()
#         print(f"Servidor: {response}")

#         # Enviar mensajes al servidor solo si es tu turno
#         if "Es tu turno" in response:
#             while True:
#                 message = input("Escribe tu mensaje (o 'PASO' para terminar tu turno): ")
#                 client_socket.send(message.encode())

#                 if message == "PASO":
#                     print("Turno terminado. Esperando tu próximo turno...")
#                     break


# if __name__ == "__main__":
#     t1 = threading.Thread(target=listener_job, args=("Listener1", get_data_1))
#     t2 = threading.Thread(target=listener_job, args=("Listener2", get_data_2))

#     t1.start()
#     t2.start()

#     t1.join()
#     t2.join()

