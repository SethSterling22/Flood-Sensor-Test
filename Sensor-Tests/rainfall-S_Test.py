from gpiozero import Button
import RPi.GPIO as GPIO

# GPIO number of the Rainfall Sensor
rain_sensor = Button(6)
count = 0

# Add one to the counter when the Sensor detect a drop
def bucket_tipped():
    global count
    count +=1
    print(count)

try:
    print("Testing Rainfall Sensor. Press Ctrl+C to exit.")
    
    while True:
        rain_sensor.when_pressed = bucket_tipped

except KeyboardInterrupt:
    print("Exiting program...")

finally:
    # Clean up GPIO settings on exits
    GPIO.cleanup()