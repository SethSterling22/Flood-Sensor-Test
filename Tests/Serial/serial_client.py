import socket
import serial
import time
import json
import sys

# --- Configuration ---
SERIAL_PORT = '/dev/ttyACM0' # Radio SiK USB Port
BAUD_RATE = 57600
SERVER_IP = '127.0.0.1' # Placeholder
SERVER_PORT = 4040
NODE_ID = "SENSOR_NODE_01"
ACK_LENGTH = 13
TEST_PAYLOAD = {"sensor_id": NODE_ID, "temp": 25.5, "humidity": 70, "time": time.time()}

def init_serial_connection():
    try:
        # Try to connect to the radio
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=5)
        print(f"📡 SUCCESS: Connected to SiK Radio on {SERIAL_PORT}")
        return ser, 'serial'
    except serial.SerialException:
        print("📡 FAIL: Could not connect to SiK Radio.")
        return None, None

def init_tcp_connection():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_IP, SERVER_PORT))
        print(f"🌐 SUCCESS: Connected to TCP Server at {SERVER_IP}:{SERVER_PORT}")
        return s, 'tcp'
    except socket.error as e:
        print(f"🌐 FAIL: Could not connect to TCP Server: {e}")
        return None, None

def send_and_wait_ack(conn, conn_type, payload):
    payload_str = json.dumps(payload)
    payload_bytes = payload_str.encode('utf-8')
    payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')
    full_payload = payload_length_bytes + payload_bytes
    
    # 1. Send dummy data
    if conn_type == 'serial':
        conn.write(full_payload)
        print(f"[{conn_type.upper()} SENT] {len(full_payload)} bytes.")
    elif conn_type == 'tcp':
        conn.sendall(full_payload)
        print(f"[{conn_type.upper()} SENT] {len(full_payload)} bytes.")
    
    # 2. Wait ACK
    try:
        if conn_type == 'serial':
            ack_bytes = conn.read(ACK_LENGTH)
        elif conn_type == 'tcp':
            ack_bytes = conn.recv(ACK_LENGTH)
        
        ack = ack_bytes.decode().strip()
        
        if ack.startswith("DATA_RECEIVED"):
            print(f"✅ ACK RECEIVED: {ack}")
            return True
        else:
            print(f"❌ UNEXPECTED RESPONSE: {ack}")
            return False

    except socket.timeout:
        print(f"❌ TIMEOUT waiting for ACK.")
        return False
    except serial.SerialTimeoutException:
        print(f"❌ SERIAL TIMEOUT waiting for ACK.")
        return False
    except Exception as e:
        print(f"❌ ERROR during ACK reception: {e}")
        return False

if __name__ == "__main__":
    # Initial logic, to choose between TCP (Socket) or serial
    force_tcp = len(sys.argv) > 1 and sys.argv[1].lower() == 'tcp'
    
    conn = None
    conn_type = None

    if force_tcp:
        conn, conn_type = init_tcp_connection()
    else:
        conn, conn_type = init_serial_connection()
        
        # Si la conexión Serial falla, intentamos TCP como respaldo
        # If Serial connection fails, use the TCP connection
        if conn is None:
            print("Trying TCP backup connection...")
            conn, conn_type = init_tcp_connection()

    if conn is None:
        print("🔴 FATAL: Cannot establish any connection. Exiting.")
        sys.exit(1)

    # Send test loops
    try:
        while True:
            # Simulate READY_TO_INDEX was received or it's time to send
            print(f"\n--- Starting data transmission via {conn_type.upper()} ---")
            send_and_wait_ack(conn, conn_type, TEST_PAYLOAD)
            time.sleep(30) # Send each 30 seconds

    except KeyboardInterrupt:
        print("Client shutting down.")
    finally:
        if conn_type == 'tcp':
            conn.close()
        elif conn_type == 'serial':
            conn.close()