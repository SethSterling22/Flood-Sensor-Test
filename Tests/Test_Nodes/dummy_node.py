"""
This program creates dummmy data
"""

import time
import random
import datetime

###########################################################
def get_data():
    """
    Generate dummy data
    """

    # Simulate variable processing time
    time.sleep(random.uniform(5, 15)) 
    now = datetime.datetime.now()
    time_string = f"{now.hour}:{now.minute}:{now.second}"
    
    return random.uniform(20, 30)
    # return {
    #     "Sensor": random.choice(["Rain Gauge", "Flood Sensor"]),
    #     "Metrics": random.uniform(20, 30),
    #     "Time": time_string
    # }

###########################################################
