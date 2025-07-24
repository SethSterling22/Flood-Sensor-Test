import RPi.GPIO as GPIO
import time

RAIN_PIN = 18  # BCM numbering

def rain_event(channel):
    print("Raindrop detected!")
while True:
    try:
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)  # Make sure you're using BCM if pin 17
        GPIO.setup(RAIN_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        
        # Now add event detection
        GPIO.add_event_detect(RAIN_PIN, GPIO.FALLING, callback=rain_event, bouncetime=300)

        print("Rain gauge monitoring started. Press Ctrl+C to stop.")
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        GPIO.cleanup()
