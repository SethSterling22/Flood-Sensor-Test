"""
This program requires a HiLetgo Module Sensor to work.
Have the option to record the Temperatura in Celcius or
Fahrenheit by just commented the formula or not.
"""

import os
import time
import logging
import datetime
import dht11 as dh
import RPi.GPIO as GPIO
from dotenv import load_dotenv



# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables

# === SENSOR SETUP ===
CHANNEL = int(os.getenv('TEMP_&_HUMID_SENSOR'))
GPIO.setmode(GPIO.BCM) # (BCM or BOARD)
instance = dh.DHT11(pin = CHANNEL)

# === LOGGING SETUP ===
logger = logging.getLogger(__name__)



def get_temp_and_humid_data():
    """
    This function register Temperature and Humidity information
    based on a HiLetgo Module Sensor.
    """
    logger.info("🌡️ Starting Temperature and Humidity monitoring...")

    try:
        while True:
            # Read data from the sensor
            result = instance.read()

            if result.is_valid():
                # Valid data received
                temperature_c = result.temperature
                humidity = result.humidity
                #now = datetime.datetime.now()
                #time_string = f"{now.hour}:{now.minute}:{now.second}"

                # Celcius string convertion
                temperature = f"{temperature_c:.1f}°C"

                # Fahrenheit convertion (uncomment the next lines to convert "temperature" to Fahrenheit)
                # temperature_f_calc = (temperature_c * 9/5) + 32
                # temperature = f"{temperature_f_calc:.1f}°F"

                #logger.info("Time: %s | Temperature: %s | Humedity: %.1f %%", time_string, temperature, humidity)
                return temperature, humidity

                # If the lecture was successful, wait 5 seconds for the next one
                time.sleep(5)
            else:
                # Los códigos de error a menudo indican un fallo temporal de lectura
                logger.error("Error reading the sensor. Code: %s. Trying again...", result.error_code)

    except Exception as e:
        logger.info("\n❌ An error has occurred with the Temperature and Humidity Sensor: \n\n %s", e)

if __name__ == "__main__":
    get_temp_and_humid_data()
