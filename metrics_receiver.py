import socket
import threading
import random
import signal
import sys
import time


import json
from datetime import datetime


from dotenv import load_dotenv
import os


# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables


HOST = "0.0.0.0"
PORT = int(os.getenv("RECEIVER_PORT") or 4040)

Nodes = {}
NODE_COUNTER = 0


def start_server():

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:

        # Make the port Reusable if it turns down
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"🟢 Server actived on: {HOST}:{PORT}")

        while True:
            conn, addr = server_socket.accept()
            with conn:
                print(f"📡 Connection stablished from: {addr}")

                # Send signal to the client when connection is stablished
                conn.sendall(b"READY")

                # Wait for the client to sent the data (Must be pushed in a queue)
                data = conn.recv(4096)
                if not data:
                    continue

                try:
                    message = json.loads(data.decode("utf-8"))
                    print(f"🕓 {datetime.now()} - Received Data:")
                    print(json.dumps(message, indent=4))
                    conn.sendall(b"OK")  # Confirmación final
                except json.JSONDecodeError:
                    print("❌ Error: Received Data is not a valid JSON format.")
                    conn.sendall(b"ERROR")

if __name__ == "__main__":
    start_server()


# # Diccionario para mantener las sesiones de pares
# sessions = {}
# server_running = True

# ###############################################################
# class SessionManager:
#     def __init__(self):
#         self.sessions = {}
#         self.counter = 1
#         self.lock = threading.Lock()

#     def add_client(self, client_socket):
#         with self.lock:
#             if len(self.sessions) % 2 == 0:
#                 session_id = self.counter
#                 self.sessions[session_id] = [client_socket]
#                 self.counter += 1
#                 return session_id, True  # Nueva sesión
#             else:
#                 session_id = self.counter - 1
#                 self.sessions[session_id].append(client_socket)

#                 self._send_seed_to_session(session_id) # Envío de semilla a las sesiones
#                 return session_id, False  # Sesión completada

#     def _send_seed_to_session(self, session_id):
#         """Envía la semilla a ambos clientes de la sesión"""
#         if session_id in self.sessions and len(self.sessions[session_id]) == 2:
#             # seed_message = f"SEMILLA:{genSeed()}".encode()
#             for client in self.sessions[session_id]:
#                 try:
#                     client.send(seed_message)
#                 except Exception as e:
#                     print(f"Error enviando semilla: {e}")

#     def remove_client(self, session_id, client_socket):
#         with self.lock:
#             if session_id in self.sessions:
#                 if client_socket in self.sessions[session_id]:
#                     self.sessions[session_id].remove(client_socket)
#                     if not self.sessions[session_id]:
#                         del self.sessions[session_id]

# session_manager = SessionManager()
# ###############################################################


# ###############################################################
# # Manejo de los clientes
# def handle_client(client_socket, client_address):
#     global server_running
    
#     try:
#         print(f"Nueva conexión desde {client_address}")

#         # Asignar el cliente a una sesión
#         session_id, is_new_session = session_manager.add_client(client_socket)
        
#         if is_new_session:
#             print(f"Esperando al segundo cliente para la sesión {session_id}")
#             client_socket.send("Esperando al segundo cliente...".encode())
#         else:
#             print(f"Sesión {session_id} iniciada con dos clientes")
            


#             # Asignar aleatoriamente el primer turno
#             first_turn = random.choice([0, 1])
#             session_manager.sessions[session_id][first_turn].send("Sesión iniciada. Es tu turno.".encode())
#             session_manager.sessions[session_id][1 - first_turn].send("Sesión iniciada. Esperando tu turno...".encode())

#         # Manejar la comunicación entre los clientes
#         while server_running:
#             try:
#                 message = client_socket.recv(1024).decode()
#                 if not message:
#                     break

#                 print(f"Mensaje recibido en la sesión {session_id}: {message}")

#                 # Enviar el mensaje al otro cliente en la sesión
#                 if session_id in session_manager.sessions:
#                     other_index = 1 if client_socket == session_manager.sessions[session_id][0] else 0
#                     if other_index < len(session_manager.sessions[session_id]):
#                         session_manager.sessions[session_id][other_index].send(message.encode())

#                     # Manejo de turnos
#                     if message == "P":
#                         session_manager.sessions[session_id][other_index].send("Es tu turno.".encode())

#             except (ConnectionResetError, ConnectionAbortedError):
#                 print(f"Cliente {client_address} desconectado")
#                 break
#             except Exception as e:
#                 print(f"Error en la sesión {session_id}: {str(e)}")
#                 break

#     finally:
#         # Eliminar el cliente de la sesión
#         session_manager.remove_client(session_id, client_socket)
#         client_socket.close()
#         print(f"Conexión con {client_address} cerrada")
# ###############################################################


# ###############################################################
# # Handler para el cierre del servidor y no quede el puerto vivo
# def signal_handler(sig, frame):
#     global server_running
#     print("\nRecibida señal de terminación, cerrando servidor...")
#     server_running = False
    
#     # Forzar cierre del socket principal
#     if 'server_socket' in globals():
#         try:
#             # Crear conexión temporal para desbloquear accept()
#             temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             temp_socket.connect(("127.0.0.1", 12345))
#             temp_socket.close()
#         except:
#             pass
        
#         try:
#             server_socket.close()
#             print("Socket del servidor cerrado correctamente")
#         except Exception as e:
#             print(f"Error cerrando socket del servidor: {str(e)}")
    
#     sys.exit(0)
# ###############################################################


# ###############################################################
# def start_weather_services():
    

    
#     global server_socket, server_running
    
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

#     # Verify if a parameter was passed in the execution of main
#     host =  "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_IP')
#     port = os.getenv('RECEIVER_PORT')
    

#     # Ejecutar los servicios de flood_sensor.py y rain_gauge.py y quedarse esperando a alguna respuesta para invocar a sus respectivas funciones que hagan fetch al Receiver !!!
#     try:
#         server_socket.bind((host, port))
#         server_socket.listen(5)
#         print(f"Servidor iniciado en {host}:{port}")
        
#         # Configuración de manejo de señales
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)
        
#         while server_running:
#             try:
#                 client_socket, client_address = server_socket.accept()
#                 client_thread = threading.Thread(
#                     target=handle_client, 
#                     args=(client_socket, client_address),
#                     daemon=True
#                 )
#                 client_thread.start()
#             except OSError as e:
#                 if server_running:
#                     print(f"Error aceptando conexión: {str(e)}")
#                     break
#     finally:
#         if 'server_socket' in globals():
#             server_socket.close()
#         print("Servidor detenido")
# ###############################################################


# if __name__ == "__main__":
#     start_weather_services()
