from gpiozero import Button

rain_sensor = Button(6)
count = 0

def bucket_tipped():
    global count
    count +=1
    print(count)

while True:
    rain_sensor.when_pressed = bucket_tipped


