import os
os.environ["GPIOZERO_PIN_FACTORY"] = "rpigpio"

from gpiozero import Button
from datetime import datetime, timezone
import logging
import csv
import time

# === CONFIGURATION ===
BUCKET_SIZE = 0.2794  # mm per tip
GPS_LAT = 60.793241544286595
GPS_LON = -161.78002508639943
LOG_DIR = "/home/wmobley/Desktop/Flood-Sensor/rain_logs"
PID_FILE = "/home/wmobley/Desktop/Flood-Sensor/flood_sensor.pid"

os.makedirs(LOG_DIR, exist_ok=True)

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR,'rain_gauge_sensor.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === WRITE PID FILE ===
with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))

# === SENSOR SETUP ===
rain_sensor = Button(6) # Previous 27

# Globals to track counts and timing
count = 0
current_hour_str = None
current_measurement_file = None
last_logged_minute = None
SENSOR_FILE = os.path.join(LOG_DIR, "sensors.csv")

def init_sensor_file_if_needed():
    if not os.path.exists(SENSOR_FILE):
        try:
            with open(SENSOR_FILE, "w", newline="") as f:
                writer = csv.writer(f, delimiter=",")
                writer.writerow(["alias","variablename","postprocess","units","datatype"])
                writer.writerow(["precipitation","precipitation","true","mm","float"])
            logger.info(f"✅ Created sensor file with headers at {SENSOR_FILE}")
        except Exception as e:
            logger.error(f"Failed to create sensor file: {e}")
    else:
        logger.info(f"Sensor file already exists at {SENSOR_FILE}")

def get_measurement_file_for_hour(dt):
    return os.path.join(LOG_DIR, f"rain_{dt.strftime('%Y%m%d_%H')}.csv")

def init_measurement_file_if_needed(filepath):
    if not os.path.exists(filepath):
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow(["Precipitation_mm","collectiontime","Lat_deg","Lon_deg"])

def bucket_tipped():
    global count
    count += 1

def log_minute_data(minute_dt, rainfall_mm):
    try:
        init_measurement_file_if_needed(current_measurement_file)
        timestamp_iso = minute_dt.replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
        with open(current_measurement_file, "a", newline="") as f:
            writer = csv.writer(f, delimiter=",")
            writer.writerow([rainfall_mm, timestamp_iso,GPS_LAT,GPS_LON])
        logger.info(f"💧 Logged {rainfall_mm:.2f} mm for minute starting {timestamp_iso} to {os.path.basename(current_measurement_file)}")
    except Exception as e:
        logger.error(f"Failed to log minute data: {e}")

def main():
    global current_hour_str, current_measurement_file, count, last_logged_minute

    logger.info("🌧️ Starting rain gauge monitor...")
    init_sensor_file_if_needed()    
    
    # Call to the RainFall Sensor Function
    #rain_sensor.when_pressed = bucket_tipped

    try:
        while True:
            now = datetime.now()
            hour_str = now.strftime('%Y%m%d_%H')
            current_minute = now.replace(second=0, microsecond=0)

            # Call to the RainFall Sensor Function
            rain_sensor.when_pressed = bucket_tipped
        

            # Rotate measurement file on hour change
            if current_hour_str != hour_str:
                current_hour_str = hour_str
                current_measurement_file = get_measurement_file_for_hour(now)
                logger.info(f"🕰️ New hour started: logging measurements to {os.path.basename(current_measurement_file)}")
                # Reset last logged minute so first minute can log properly
                last_logged_minute = None

            # Check if we moved to a new minute (and last_logged_minute is set)
            if last_logged_minute is None:
                last_logged_minute = current_minute

            elif current_minute > last_logged_minute:
                # Time to log previous minute's rainfall
                rainfall_mm = count * BUCKET_SIZE
                log_minute_data(last_logged_minute, rainfall_mm)
                count = 0  # Reset for new minute
                last_logged_minute = current_minute

            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("🌧️ Rain gauge monitoring stopped by user.")

if __name__ == "__main__":
    main()
