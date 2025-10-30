"""
This program creates dummmy data
"""

import time
import random
from datetime import datetime

###########################################################
def get_data():
    """
    Generate dummy data
    """

    # Simulate variable processing time
    time.sleep(random.uniform(0.1, 4)) 

    return {
        "Sensor": random.choice(["Rain Gauge", "Flood Sensor"]),
        "Metrics": random.uniform(20, 30),
        "Dummy": random.uniform(40, 60),
        "status": random.choice(["OK", "FLOODING"])
    }
###########################################################
