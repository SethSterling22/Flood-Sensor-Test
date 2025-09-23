from gpiozero import Button

# GPIO number of the Rainfall Sensor
rain_sensor = Button(6)
count = 0

# Add one to the counter when the Sensor detect a drop
def bucket_tipped():
    global count
    count +=1
    print(count)

try:
    print("Testing liquid sensor. Press Ctrl+C to exit.")
    
    while True:
        rain_sensor.when_pressed = bucket_tipped

except KeyboardInterrupt:
    print("Exiting program.")

finally:
    # Clean up GPIO settings on exit
    GPIO.cleanup()