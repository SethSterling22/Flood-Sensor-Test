import RPi.GPIO as GPIO
import dht11
import time
import sys
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

print("Iniciando lectura del DHT11. Presiona Ctrl+C para salir.")

try:
    while True:
        # Lee los datos del sensor
        result = instance.read()

        if result.is_valid():
            # Datos válidos recibidos
            temperature_c = result.temperature
            humidity = result.humidity
            
            print(
                f"Temperatura: {temperature_c:.1f}°C | Humedad: {humidity:.1f}%"
            )
        else:
            # Los códigos de error a menudo indican un fallo temporal de lectura
            print(f"Error al leer el sensor. Code: {result.error_code}. Reintentando...")

        # El DHT11 solo debe leerse una vez cada 2 segundos como mínimo.
        time.sleep(2.0)

except KeyboardInterrupt:
    print("\nPrograma terminado por el usuario.")
    
except Exception as e:
    print(f"\nError inesperado: {e}")

finally:
    # Es crucial limpiar la configuración GPIO al salir
    GPIO.cleanup()
    sys.exit(0)