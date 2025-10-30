"""
This program is created to test the multi-node connection
and the synchronization with the server
"""


# import os
# import sys
# import time
# import json
# import socket
# import random
# import threading
# from datetime import datetime
# from dotenv import load_dotenv
# from dummy_node import get_data as dummy_data

# # ===  ENVIRONMENT  VARIABLES ===
# load_dotenv("../../Env/.env.config")
# RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
# RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
# NODE_ID = "Dummy"  # Identifcate the test

# print(RECEIVER_HOST, RECEIVER_PORT)

# # === TEST CONFIGURATION ===
# MIN_NODES = 2
# MAX_NODES = 6
# MIN_SEND_INTERVAL = 10  # Seconds
# MAX_SEND_INTERVAL = 30  # Seconds
# TEST_DURATION = 60      # Seconds (1 minutes)
# DISCONNECT_PROBABILITY = 0.2  # 20% probability to get disconnected

# class DummyNode(threading.Thread):
#     def __init__(self, node_id):
#         threading.Thread.__init__(self)
#         self.node_id = f"{NODE_ID}_{node_id}"
#         self.running = True
#         self.connection_count = 0
        
#     def run(self):
#         while self.running and time.time() < end_time:
#             try:
#                 # Decide if the node gets disconnected
#                 if random.random() < DISCONNECT_PROBABILITY:
#                     print(f"[{self.node_id}] 🔄 Simulating random disconnection.")
#                     time.sleep(random.randint(5, 10))
#                     continue
                
#                 # Connect and send data
#                 with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                     s.settimeout(15)
#                     self.connection_count += 1
                    
#                     print(f"[{self.node_id}] 🔌 Connection #{self.connection_count}")
#                     s.connect((RECEIVER_HOST, RECEIVER_PORT))
                    
#                     # Step 1. Identification
#                     s.sendall(self.node_id.encode('utf-8'))
#                     handshake = s.recv(1024).decode("utf-8")
#                     if handshake != "READY":
#                         print(f"[{self.node_id}] ❌ Handshake error")
#                         continue
                        
#                     # Step 2. Send data
#                     data = dummy_data()
#                     payload = {
#                         "thread": f"Sensor-{self.node_id}",
#                         "timestamp": datetime.now().isoformat(),
#                         "data": data
#                     }
#                     s.sendall(json.dumps(payload).encode("utf-8"))
                    
#                     # Step 3. Wait for response
#                     response = s.recv(1024).decode("utf-8")
#                     print(f"[{self.node_id}] 📤 Sent: {data} | 📥 Response: {response}")
                    
#                 # Wait random interval
#                 time.sleep(random.uniform(MIN_SEND_INTERVAL, MAX_SEND_INTERVAL))
                
#             except Exception as e:
#                 print(f"[{self.node_id}] ⚠️ Error: {str(e)}")
#                 time.sleep(5)
                
#     def stop(self):
#         self.running = False

# if __name__ == "__main__":
#     print(f"🚀 Starting stress test with {MIN_NODES}-{MAX_NODES} nodes")
#     print(f"⏱ Duration: {TEST_DURATION} Seconds | 📶 Disconnect probability: {DISCONNECT_PROBABILITY*100}%")
    
#     end_time = time.time() + TEST_DURATION
#     nodes = []
#     num_nodes = random.randint(MIN_NODES, MAX_NODES)
    
#     # Create nodes
#     for i in range(num_nodes):
#         node = DummyNode(i+1)
#         node.start()
#         nodes.append(node)
#         time.sleep(1)  # Scale start
    
#     # Wait test time
#     try:
#         while time.time() < end_time:
#             time.sleep(5)

#             # Show summary each 5 seconds
#             active_nodes = sum(1 for node in nodes if node.running)
#             total_connections = sum(node.connection_count for node in nodes)
#             print(f"\n📊 Summary: {active_nodes}/{num_nodes} active nodes | 📨 {total_connections} total connections\n")
#     except KeyboardInterrupt:
#         print("\n🛑 Test stopped by user...")
    
#     # Detener nodos
#     for node in nodes:
#         node.stop()
#         node.join()
    
#     print("✅ Test completed. Final stats:")
#     for node in nodes:
#         print(f"🆔 {node.node_id}: {node.connection_count} successful connections")


import os
import sys
import time
import json
import socket
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv


# Import sensor functions
from dummy_node import get_data as dummy_data



# === ENVIRONMENT VARIABLES ===
load_dotenv("../../Env/.env.config")
# LOG_DIR = "./Logs/"
# os.makedirs(LOG_DIR, exist_ok=True)

# Connection settings
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
NODE_ID = "Dummy"


# === LOGGING SETUP ===
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(os.path.join(LOG_DIR, 'sensor_node.log')),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)
STOP_EVENT = threading.Event()


def send_to_receiver(thread_name, data):
    """Send sensor data to the receiver server"""
    payload = {
        "thread": thread_name,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10)
            
            print(f"📡 Connecting to server at {RECEIVER_HOST}:{RECEIVER_PORT}")
            s.connect((RECEIVER_HOST, RECEIVER_PORT))

            # Node identification
            id_request = s.recv(1024)
            if id_request != b"NODE_ID_REQUEST":
                print("❌ Protocol error: expected ID request")
                return False

            s.sendall(NODE_ID.encode('utf-8'))
            response = s.recv(1024)
            
            if response != b"READY":
                print(f"❌ Server not ready: {response.decode()}")
                return False

            # Send data
            s.sendall(json.dumps(payload).encode('utf-8'))
            final_response = s.recv(1024).decode('utf-8')
            
            if final_response == "OK_QUEUED":
                print(f"✅ Data queued at server ({thread_name})")
                return True
            else:
                print(f"⚠️ Server response: {final_response}")
                return False

    except socket.timeout:
        print("⌛ Connection timeout with server")
    except ConnectionRefusedError:
        print("🔌 Connection refused - server may be down")
    except Exception as e:
        print(f"🔴 Unexpected error: {str(e)}")
    return False


def sensor_job(thread_name, func):
    """Thread worker for each sensor"""
    retry_count = 0
    max_retries = 3
    
    while not STOP_EVENT.is_set():
        try:
            data = func()
            print(f"[{thread_name}] Collected data: {data}")
            
            success = send_to_receiver(thread_name, data)
            
            if not success and retry_count < max_retries:
                retry_count += 1
                print(f"[{thread_name}] Retry {retry_count}/{max_retries} in 30s...")
                STOP_EVENT.wait(30)  # Wait for retry
                continue
                
            retry_count = 0
            STOP_EVENT.wait(60)  # Normal 60-second interval
            
        except Exception as e:
            print(f"[{thread_name}] Critical error: {str(e)}")
            STOP_EVENT.wait(60)  # Wait before next attempt


if __name__ == "__main__":
    # Start sensor threads
    sensors = [
        threading.Thread(target=sensor_job, args=("🌧️ Rain Gauge", dummy_data)),
        threading.Thread(target=sensor_job, args=("💧 Flood Sensor", dummy_data))
    ]

    for sensor in sensors:
        sensor.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Stopping all sensors...")
        STOP_EVENT.set()
        for sensor in sensors:
            sensor.join()
        print("👋 All sensors stopped")
        sys.exit(0)