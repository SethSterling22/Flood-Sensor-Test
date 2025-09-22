import RPi.GPIO as GPIO
import time

# Use BCM numbering mode
GPIO.setmode(GPIO.BCM)

# Define the GPIO pin. In your case, pin 13.
sensor_pin = 13

# Set up the sensor pin as an input with a pull-up resistor.
# This prevents the pin from "floating" and giving false readings.
GPIO.setup(sensor_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

try:
    print("Testing liquid sensor. Press Ctrl+C to exit.")
    
    while True:
        # Read the pin state.
        # It will be LOW when liquid is detected.
        if GPIO.input(sensor_pin) == GPIO.LOW:
            print("Liquid detected!")
        else:
            print("No liquid detected.")
        
        # Add a small delay
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Exiting program.")

finally:
    # Clean up GPIO settings on exit
    GPIO.cleanup()

