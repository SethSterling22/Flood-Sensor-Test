"""
This program will serve as the main thread for "main.py" 
to detect rain with a precipitation sensor. If precipitation 
is detected, the counter will increase by 1. After one minute, 
the resulting sum will be sent to "main.py" to be calculated 
as an hourly average and stored.

"main.py" will storage each hour the precipiation average to be
sent to Upstream-dso each day. If the average of the mm per minute 
from the past 60 logs is more than 10mm it will send all the 
information that it have in queue to send that day
"""

import os
import time
import logging
from gpiozero import Button
from dotenv import load_dotenv



# === ENVIRONMENT  VARIABLES ===
load_dotenv("../Env/.env.config")  # Config env variables

# === CONFIGURATION ===
BUCKET_SIZE = float(os.getenv('BUCKET_SIZE'))  # mm per tip, adjust if needed

# === SENSOR SETUP ===
rain_sensor = Button(int(os.getenv('RAINFALL_SENSOR'))) # Previous 27

# === GLOBALS SETUP to track counts and timing ===
count = 0

# === LOGGING SETUP ===
logger = logging.getLogger(__name__)
# Configuración básica para ver los mensajes en la consola
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def bucket_tipped():
    """
    This function is called by the sensor event. It only increase the counter.
    """
    global count
    count += 1

# Configure the event Manager of the Sensor
rain_sensor.when_pressed = bucket_tipped


def get_rain_data():
    """
    Counts the ammount of precipitation per minute and return the result in mm.
    """
    global count
    # Count the total number in the 60 seconds interval
    current_count = count

    try:
        # Calculate the count by the bucket size to get the volume
        minute_tips = current_count * BUCKET_SIZE 

        # Restart the counter
        count = 0
        
        return minute_tips

    except Exception as e:
        logger.info("\n❌ An error has occurred with the Rain Sensor: \n\n %s", e)
        return 0


def run_accumulation_test(duration_seconds=60):
    """
    Simulate the job for a 60 seconds collection
    """
    global count

    logger.info("--- 🧪 ACCUMULATION TEST %d SECONDS ---" % duration_seconds)
    logger.info("💧 Raingauge Counter started")
    logger.info("   Initial counter: %d" % count)

    start_time = time.time()

    while time.time() - start_time < duration_seconds:
        time.sleep(1) 
        
    # Call get_rain_data()
    total_tips_accumulated = count
    logger.info("⏱️ End of the period of  %d seconds." % duration_seconds)

    final_result_mm = get_rain_data()
    # Show results
    logger.info("-" * 40)
    logger.info("✅ Results of the collection cycle:")
    logger.info("   Total Counts (Tips): %d" % total_tips_accumulated)
    logger.info("   Precipitation accumulated: %.3f mm" % final_result_mm)
    logger.info("   Global counter after reset: %d" % count)
    logger.info("-" * 40)

if __name__ == "__main__":
    # Ejecuta el ciclo de prueba de 60 segundos
    run_accumulation_test(duration_seconds=60)



########################### Old version ###########################
# current_hour_str = None
# current_measurement_file = None
# last_logged_minute = None


# # === PRINCIPAL FUNCTIONS ===
# def log_minute_data():
#     """Responsible for logging data every minute."""
#     global count, last_logged_minute, current_measurement_file

#     now = datetime.now()
#     current_minute = now.replace(second=0, microsecond=0)
#     hour_str = now.strftime('%Y%m%d_%H')

#     if current_measurement_file is None or os.path.basename(current_measurement_file) != f"rain_{hour_str}.csv":
#         current_measurement_file = get_measurement_file_for_hour(now)
#         logger.info(f"🕰️ New hour started: logging measurements to {os.path.basename(current_measurement_file)}")
#         last_logged_minute = current_minute

#     elif current_minute > last_logged_minute:
#         # Log the data from the previous minute
#         rainfall_mm = count * BUCKET_SIZE
#         try:
#             init_measurement_file_if_needed(current_measurement_file)
#             timestamp_iso = last_logged_minute.replace(tzinfo=timezone.utc).isoformat()
#             with open(current_measurement_file, "a", newline="") as f:
#                 writer = csv.writer(f, delimiter=",")
#                 writer.writerow([rainfall_mm, timestamp_iso, GPS_LAT, GPS_LON])
#             logger.info(f"💧 Logged {rainfall_mm:.2f} mm for minute starting {timestamp_iso} to {os.path.basename(current_measurement_file)}")
#         except Exception as e:
#             logger.error(f"Failed to log minute data: {e}")

#         count = 0  # Restart the counter
#         last_logged_minute = current_minute

#     # Configure the next 60 seconds
#     t = Timer(60, log_minute_data)
#     t.start()
########################### Old version ###########################
