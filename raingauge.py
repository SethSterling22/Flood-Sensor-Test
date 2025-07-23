import RPi.GPIO as GPIO
import time

RAIN_PIN = 17  # or whatever GPIO pin you're using

rain_count = 0

def rain_event(channel):
    global rain_count
    rain_count += 1
    print(f"Raindrop detected! Total: {rain_count}")

GPIO.setmode(GPIO.BCM)
GPIO.setup(RAIN_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)

# Detect falling edge (tip of the bucket)
GPIO.add_event_detect(RAIN_PIN, GPIO.FALLING, callback=rain_event, bouncetime=300)

print("Rain gauge monitoring started. Press Ctrl+C to stop.")
try:
    while True:
        time.sleep(60)
        print(f"Rainfall total: {rain_count * 0.2794} mm")
except KeyboardInterrupt:
    print("Exiting...")
finally:
    GPIO.cleanup()
