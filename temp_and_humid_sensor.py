import RPi.GPIO as GPIO
import dht11
import time
import sys
import os
from dotenv import load_dotenv


# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.config")  # Config env variables

# Define el modo de numeración de pines (BCM o BOARD)
# Usaremos BCM para referirnos al número GPIO.
GPIO.setmode(GPIO.BCM) 

# Define el número BCM del pin al que está conectado el cable DATA del DHT11
# CHANNEL = 4  # Pin Físico 7 (GPIO BCM 4)

# === SENSOR SETUP ===
CHANNEL = int(os.getenv('TEMP_&_HUMID_SENSOR'))

# Inicializa el sensor DHT11 con el pin de datos (canal)
instance = dht11.DHT11(pin = CHANNEL)

print("Start recording... Press Ctrl+C to stop.")

try:
    while True:
        # Lee los datos del sensor
        result = instance.read()

        if result.is_valid():
            # Datos válidos recibidos
            temperature_c = result.temperature
            humidity = result.humidity
            
            print(
                f"Temperature: {temperature_c:.1f}°C | Humedity: {humidity:.1f}%"
            )
            time.sleep(5)
        else:
            # Los códigos de error a menudo indican un fallo temporal de lectura
            print(f"Error reading the sensor. Code: {result.error_code}. Trying again...")

        # El DHT11 solo debe leerse una vez cada 2 segundos como mínimo.
        

except KeyboardInterrupt:
    print("\nProgram stopped by user.")
    
except Exception as e:
    print(f"\nUnexpected Error: {e}")

finally:
    # Es crucial limpiar la configuración GPIO al salir
    GPIO.cleanup()
    sys.exit(0)