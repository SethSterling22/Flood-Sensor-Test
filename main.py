"""
This program deploys two threads to "flood_sensor.py" and 
"rain_gauge.py" to collect the various signals they send, 
while also synchronizing and packaging that information. 
It also connects to the server running "metrics_receiver.py," 
either locally or externally. "main.py" maintains the connection 
to the server and sends the previously packaged information 
from both sensors.
"""



import os
import sys
import time
import json
import socket
import logging
import datetime
import threading
from dotenv import load_dotenv


# Import sensor functions
##############################################################
from Sensors.rain_gauge import get_rain_data as rain_gauge_data
from Sensors.flood_sensor import get_flood_data as flood_sensor_data
from Sensors.temp_and_humid_sensor import get_temp_and_humid_data as temp_humid_data
##############################################################



# === ENVIRONMENT VARIABLES ===
load_dotenv("./Env/.env.config")
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)


# === CONNETION SETTINGS ===
RECEIVER_HOST = "127.0.0.1" if len(sys.argv) > 1 else os.getenv('RECEIVER_HOST')
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "4040"))
#NODE_ID = "NODE_Dummy1"  # Must start with "NODE_"
NODE_ID = f"NODE_{os.getenv('NODE_ID', 'default')}"


# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'main_client.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# === GLOBAL VARIABLES ===
BUFFER_LOCK = threading.Lock()
SENSOR_DATA_BUFFER = [] 
CLIENT_READY = False
STOP_EVENT = threading.Event()



# === Thread Sensor Function ===
def listener_job(sensor_name, func):
    """
    Manages sensor data collection and 
    append it to the BUFFER.
    """

    global CLIENT_READY
    logger.info("%s started.", sensor_name)
    
    # Receive the information from the Sensors
    while not STOP_EVENT.is_set():
        try:
            # CLIENT is READY when NODE_ID is received by the Sever
            if CLIENT_READY:
                # Call to the function
                data = func()
                with BUFFER_LOCK:
                    now = datetime.datetime.now()
                    time_string = f"{now.hour}:{now.minute}:{now.second}"
                    SENSOR_DATA_BUFFER.append({
                        'sensor': sensor_name,
                        'timestamp': time_string,
                        'value': data
                    })
                    logger.debug("Buffered %s data: %.2f", sensor_name, data)
                # Wait for 60 seconds (the collection interval)
                STOP_EVENT.wait(60)

        except Exception as e:
            logger.error("%s thread error: %s", sensor_name, str(e))
            time.sleep(5)  # Wait before retrying



def client():
    """
    Manages the connection and the messages from the server.
    """

    MAX_RETRY_COUNT = 3
    SHORT_WAIT_TIME = 15
    LONG_WAIT_TIME = 300  # 5 minutes
    retry_count = 0

    global CLIENT_READY
    while not STOP_EVENT.is_set():
        # 1. Try connection to server
        try:
            # "With" statements makes socket close automatically
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(300) 
                logger.info("📡 Connecting to %s:%d (Attempt %d/%d)", RECEIVER_HOST, RECEIVER_PORT, retry_count + 1, MAX_RETRY_COUNT)
                s.connect((RECEIVER_HOST, RECEIVER_PORT))

                # Wait until server connects and send the CONNECTED message
                response = s.recv(9).decode().strip()
                logger.info("📡 SERVER response on Connection: %s", response)

                # Check the connection message
                if response != "CONNECTED":
                    logger.error("⚠️ Error while connecting on server: %s", response)
                    raise socket.error(f"Server sent unexpected initial response: '{response}'")

                # Restart counter if is CONNECTED
                retry_count = 0

                # 2. Send the NODE_ID to index in the Server
                s.sendall(NODE_ID.encode('utf-8'))
                
                # Read the server message
                response_bytes = s.recv(11) 
                response = response_bytes.decode().strip()
                logger.info("📡 SERVER respond with: %s", response)


                # === CONNECTION STABLISHED AND NODE REGISTERED ===
                if response.startswith("ID_RECEIVED"):

                    CLIENT_READY = True
                    logger.info("✅ Connection established and ID registered. Starting data collection... 📊")
                    
                    if len(response) > 11: # 'ID_RECEIVED' tiene 11 caracteres
                        logger.warning("⚠️ Extra command data received during registration: %s", response[11:])

                else:
                    logger.error("⚠️ NODE ID not indexed: %s", response)
                    raise socket.error(f"NODE ID not indexed. Server response: '{response}'")
                # === CONNECTION STABLISHED AND NODE REGISTERED ===


                # 3. Principal receiver loop and data sending
                while not STOP_EVENT.is_set():
                    try:
                        # Waits one second to check STOP_EVENT
                        s.settimeout(65)

                        # Wait one minute for the READY_TO_INDEX from the server
                        try:
                            # Just read the 14 bytes of "READY_TO_INDEX"
                            message_bytes = s.recv(14) 

                            # If there's no bytes (timeout), retry
                            if not message_bytes:
                                continue
                            # Receive the message
                            message = message_bytes.decode().strip()

                        except socket.timeout:
                            continue

                        # If server is ready to index:
                        if message.startswith("READY_TO_INDEX"):
                            logger.info("⏰ Server sent READY_TO_INDEX. Preparing to send data...")

                            try:
                                s.settimeout(0.01) # Timeout muy corto (10 ms)
                                drain_bytes = s.recv(30) 
                                if drain_bytes:
                                    logger.warning("🌊 Drained residual signal after ID_RECEIVED: %s", drain_bytes.decode().strip())
                                    
                            except socket.timeout:
                                # OK, socket limpio
                                pass
                            except Exception as e:
                                logger.debug("Error draining buffer after ID_RECEIVED: %s", e)


                            # Get and clean BUFFERED data
                            with BUFFER_LOCK:

                                # Copy the buffer in a tmp variable
                                data_to_send = SENSOR_DATA_BUFFER.copy()

                            # PAYLOAD BUILDING 
                            if data_to_send:
                                # 1. Serialize structured data to JSON format
                                try:
                                    payload_str = json.dumps(data_to_send)
                                    logger.info("📤 Sending %s data points.", len(data_to_send))
                                    logger.info("DATA sent:\n %s", data_to_send)
                                except TypeError as e:
                                    logger.error("⚠️ Error serializing JSON. Check data format: %s", e)
                                    break 

                            else:
                                # 2. 'NO_DATA' escenario (str)
                                logger.info("📝 Buffer empty. Sending 'NO_DATA'.")
                                payload_str = "NO_DATA"

                            # 3. CODIFICATION AND PREPARATION OF LENGTH PROTOCOL 
                            payload_bytes = payload_str.encode('utf-8')
                            payload_length_bytes = str(len(payload_bytes)).zfill(8).encode('utf-8')

                            # 4. Send (Length + Payload)
                            s.sendall(payload_length_bytes) 
                            s.sendall(payload_bytes)       

                            # 5. Wait Server confirmation
                            s.settimeout(30)
                            # 13 bytes of DATA_RECEIVED
                            ack_bytes = s.recv(13)
                            ack = ack_bytes.decode().strip()
                            s.settimeout(1) 

                            if ack.startswith("DATA_RECEIVED"):
                                # DATA was received
                                logger.info("👍 Data successfully indexed by server. [%s]", ack)

                                # Clear buffer
                                with BUFFER_LOCK:
                                    SENSOR_DATA_BUFFER.clear() 

                                # Set back the long timeout
                                s.settimeout(65)

                            elif ack == "JSON_ERROR":
                                logger.error("❌ Server failed decoding JSON data. The data was not saved.")
                                break
                            else:
                                logger.error("❌ Server ACK error receiving data: %s", ack)
                                break

                        elif message:
                            logger.warning("Received unknown message: %s", message)

                    except socket.timeout:
                        continue

                    except ConnectionResetError:
                        logger.error("🚫 Connection lost (Server closed the connection).")
                        break

                    except Exception as e:
                        logger.error("🔌 Fatal error during communication: %s", e)
                        break

        # Error management
        except socket.error as e:
            logger.error("❌ Failed to connect to server: %s", e)
            CLIENT_READY = False 

            # Increase the counter
            retry_count += 1

            if retry_count <= MAX_RETRY_COUNT:
                logger.info("⏳ Waiting %d seconds before next retry...", SHORT_WAIT_TIME)
                time.sleep(SHORT_WAIT_TIME)
            else:
                logger.info("😴 Failed %d times. Waiting %d seconds (5 minutes) before resetting attempts...", 
                            MAX_RETRY_COUNT, LONG_WAIT_TIME)
                time.sleep(LONG_WAIT_TIME)
                retry_count = 0 

    CLIENT_READY = False
    logger.info("🔌 Client thread terminated.")



if __name__ == "__main__":

    # Sensor Start
    sensors = [
        threading.Thread(target=listener_job, args=("Rain Gauge", rain_gauge_data)),
        threading.Thread(target=listener_job, args=("Flood Sensor", flood_sensor_data)),
        threading.Thread(target=listener_job, args=("Temperature and Humidity", temp_humid_data))
    ]

    for sensor in sensors:
        sensor.start()

    # Start Client on thread to do not block main
    client_thread = threading.Thread(target=client)
    client_thread.start()

    try:
        while True:
            time.sleep(1) # Principal thread waits
    except KeyboardInterrupt:
        logger.info("🛑 Stopping all threads...")
        STOP_EVENT.set()
    finally:
        for sensor in sensors:
            sensor.join()

        client_thread.join() # Wait until the client stop
        logger.info("👋 All threads stopped")
        sys.exit(0)

