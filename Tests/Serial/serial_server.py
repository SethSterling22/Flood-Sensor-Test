import socket
import serial
import threading
import time
import json

# --- CONFIGURATION ---
TCP_HOST = '0.0.0.0'
TCP_PORT = 4040
SERIAL_PORT = '/dev/ttyACM0'  # USB Radio mounted
BAUD_RATE = 57600
ACK_MSG = "DATA_RECEIVED"

def handle_serial_data(ser):
    """
    Maneja la recepción de datos a través del puerto serial (Radio SiK).
    """
    print(f"[SERIAL] Listening on {SERIAL_PORT}...")
    try:
        while True:
            # 1. Wait the 8 bytes prefix
            length_bytes = ser.read(8)
            if not length_bytes:
                # Re-try if there's no bytes
                time.sleep(0.1)
                continue

            try:
                data_length = int(length_bytes.decode())
            except ValueError:
                print(f"[SERIAL ERROR] Invalid length prefix received: {length_bytes.decode()}")
                continue

            # 2. Read the entire payload
            payload_bytes = ser.read(data_length)

            if len(payload_bytes) != data_length:
                print(f"[SERIAL ERROR] Payload size mismatch. Expected {data_length}, got {len(payload_bytes)}.")
                continue

            payload = payload_bytes.decode()

            # 3. Data processing
            print(f"\n[SERIAL RECV] Length: {data_length}, Payload: {payload}")

            # 4. Send ACK
            ack_msg = ACK_MSG.ljust(13).encode() # Fill 13 bytes to ACK
            ser.write(ack_msg)
            print(f"[SERIAL SENT] ACK: {ACK_MSG}")

    except serial.SerialException as e:
        print(f"[SERIAL FATAL] Serial port error: {e}. Thread exiting.")
    except Exception as e:
        print(f"[SERIAL FATAL] Unexpected error: {e}")

def handle_tcp_client(conn, addr):
    """
    Maneja un cliente conectado vía TCP/IP.
    """
    print(f"[TCP] New connection from {addr}")
    try:
        while True:
            # 1. Wait prefix 8 bytes
            length_bytes = conn.recv(8)
            if not length_bytes:
                print(f"[TCP] Client {addr} disconnected.")
                break
            
            try:
                data_length = int(length_bytes.decode())
            except ValueError:
                print(f"[TCP ERROR] Invalid length prefix from {addr}.")
                break
            
            # 2. Read the entire payload
            payload_bytes = conn.recv(data_length)
            payload = payload_bytes.decode()
            
            # 3. Procesar datos
            print(f"\n[TCP RECV] Length: {data_length}, Payload: {payload}")

            # 4. Send ACK
            ack_msg = ACK_MSG.ljust(13).encode()
            conn.sendall(ack_msg)
            print(f"[TCP SENT] ACK sent to {addr}")
            
    except socket.timeout:
        print(f"[TCP TIMEOUT] Connection with {addr} timed out.")
    except Exception as e:
        print(f"[TCP ERROR] Error handling {addr}: {e}")
    finally:
        conn.close()

def start_tcp_server():
    """
    Inicia el servidor de escucha TCP/IP.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((TCP_HOST, TCP_PORT))
        s.listen(5)
        print(f"[TCP] Listening on {TCP_HOST}:{TCP_PORT}")
        
        while True:
            try:
                conn, addr = s.accept()
                threading.Thread(target=handle_tcp_client, args=(conn, addr)).start()
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[TCP ERROR] Accept error: {e}")

if __name__ == "__main__":
    # Try to start the Serial thread
    try:
        serial_conn = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=5)
        serial_thread = threading.Thread(target=handle_serial_data, args=(serial_conn,))
        serial_thread.daemon = True
        serial_thread.start()
    except serial.SerialException:
        print(f"[SERIAL WARNING] Cannot connect to radio on {SERIAL_PORT}. Starting without Serial support.")

    # Start "TCP server" (Block)
    start_tcp_server()