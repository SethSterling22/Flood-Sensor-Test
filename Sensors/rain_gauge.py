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
import threading
from gpiozero import Button
from dotenv import load_dotenv



# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables

# === CONFIGURATION ===
BUCKET_SIZE = float(os.getenv('BUCKET_SIZE'))  # mm per tip, adjust if needed

# === SENSOR SETUP ===
rain_sensor = Button(int(os.getenv('RAINFALL_SENSOR'))) # Previous 27

# === GLOBALS SETUP to track counts and timing ===
global_count = 0
count_lock = threading.Lock()

# === LOGGING SETUP ===
logger = logging.getLogger(__name__)



def bucket_tipped():
    """
    This function is called by the sensor event. It only increase the counter.
    """
    global global_count
    # Usar el lock para incrementar el contador de forma segura
    with count_lock:
        global_count += 1
    # logger.debug(f"Tip registrado. Total: {global_count}")


# def get_rain_data():
#     """
#     Counts the ammount of precipitation per minute and return the result in mm.
#     """
#     global global_count
#     initial_count = global_count

#     try:
#         # Configure the event Manager of the Sensor
#         rain_sensor.when_pressed = bucket_tipped

#         # Count the total number in the 60 seconds interval
#         current_count = global_count

#         # Calculate the difference to know the ammount of rain in the previous minute
#         minute_tips = (current_count - initial_count) * BUCKET_SIZE
#         #logger.info(f"Logged {minute_tips} mm for the previous minute")

#         # Restart "count" for the next cicle
#         global_count = 0

#         return minute_tips

#     except Exception as e:
#         logger.info("\n❌ An error has occurred with the Rain Sensor: \n\n %s", e)

# if __name__ == "__main__":
#     get_rain_data()




def calculate_and_reset_rainfall():
    """
    Calcula la lluvia acumulada y resetea el contador para el siguiente minuto.
    """
    global global_count

    # 1. Adquirir el Lock para asegurar que la lectura y el reseteo sean atómicos
    with count_lock:
        # Capturar el conteo del último minuto
        minute_tips_count = global_count 
        
        # Reset the counter
        global_count = 0 

    # 2. Calcular la lluvia (fuera del Lock)
    minute_rainfall_mm = minute_tips_count * BUCKET_SIZE


    return minute_rainfall_mm


def get_rain_data():
    """
    Principal thread function, collect sync by time
    """

    # Configure the sensor
    rain_sensor.when_pressed = bucket_tipped

    current_time_seconds = time.time()
    seconds_until_next_minute = 60.0 - (current_time_seconds % 60.0)
    time.sleep(seconds_until_next_minute)

    while True:
        try:
            # Exectute the data collection
            print(calculate_and_reset_rainfall())

            # Wait a minute to reset
            time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            break
        except Exception as e:
            logger.error("❌ Thread error in collection %s", e)
            time.sleep(5)


if __name__ == "__main__":
    get_rain_data()

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


