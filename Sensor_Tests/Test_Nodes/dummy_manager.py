"""
This program is created to test the multi-node connection
and the synchronization with the server
"""


import os
import sys
import time
import json
import socket
import random
import threading
from datetime import datetime
from dotenv import load_dotenv
from dummy_node import get_data as dummy_data

# ===  ENVIRONMENT  VARIABLES ===
load_dotenv("../../Env/.env.config")
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
NODE_ID_PREFIX = os.getenv("NODE_PREFIX", "dummy")  # Identifcate the test

# === TEST CONFIGURATION ===
MIN_NODES = 2
MAX_NODES = 6
MIN_SEND_INTERVAL = 10  # Seconds
MAX_SEND_INTERVAL = 30  # Seconds
TEST_DURATION = 300     # Seconds (5 minutes)
DISCONNECT_PROBABILITY = 0.2  # 20% probability to get disconnected

class DummyNode(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = f"{NODE_ID_PREFIX}_{node_id}"
        self.running = True
        self.connection_count = 0
        
    def run(self):
        while self.running and time.time() < end_time:
            try:
                # Decide if the node gets disconnected
                if random.random() < DISCONNECT_PROBABILITY:
                    print(f"[{self.node_id}] 🔄 Simulando desconexión aleatoria")
                    time.sleep(random.randint(5, 15))
                    continue
                
                # Connect and send data
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(10)
                    self.connection_count += 1
                    
                    print(f"[{self.node_id}] 🔌 Conexión #{self.connection_count}")
                    s.connect((RECEIVER_HOST, RECEIVER_PORT))
                    
                    # Step 1. Identification
                    s.sendall(self.node_id.encode('utf-8'))
                    handshake = s.recv(1024).decode("utf-8")
                    if handshake != "READY":
                        print(f"[{self.node_id}] ❌ Handshake fallido")
                        continue
                        
                    # Step 2. Send data
                    data = dummy_data()
                    payload = {
                        "thread": f"Sensor-{self.node_id}",
                        "timestamp": datetime.now().isoformat(),
                        "data": data
                    }
                    s.sendall(json.dumps(payload).encode("utf-8"))
                    
                    # Step 3. Wait for response
                    response = s.recv(1024).decode("utf-8")
                    print(f"[{self.node_id}] 📤 Sent: {data} | 📥 Response: {response}")
                    
                # Wait random interval
                time.sleep(random.uniform(MIN_SEND_INTERVAL, MAX_SEND_INTERVAL))
                
            except Exception as e:
                print(f"[{self.node_id}] ⚠️ Error: {str(e)}")
                time.sleep(5)
                
    def stop(self):
        self.running = False

if __name__ == "__main__":
    print(f"🚀 Starting stress test with {MIN_NODES}-{MAX_NODES} nodes")
    print(f"⏱ Duration: {TEST_DURATION} Seconds | 📶 Disconnect probability: {DISCONNECT_PROBABILITY*100}%")
    
    end_time = time.time() + TEST_DURATION
    nodes = []
    num_nodes = random.randint(MIN_NODES, MAX_NODES)
    
    # Create nodes
    for i in range(num_nodes):
        node = DummyNode(i+1)
        node.start()
        nodes.append(node)
        time.sleep(1)  # Scale start
    
    # Wait test time
    try:
        while time.time() < end_time:
            time.sleep(5)

            # Show summary each 5 seconds
            active_nodes = sum(1 for node in nodes if node.running)
            total_connections = sum(node.connection_count for node in nodes)
            print(f"\n📊 Summary: {active_nodes}/{num_nodes} active nodes | 📨 {total_connections} total connections\n")
    except KeyboardInterrupt:
        print("\n🛑 Test stopped by user...")
    
    # Detener nodos
    for node in nodes:
        node.stop()
        node.join()
    
    print("✅ Test completed. Final stats:")
    for node in nodes:
        print(f"🆔 {node.node_id}: {node.connection_count} successful connections")