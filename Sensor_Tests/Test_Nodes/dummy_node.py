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

    time.sleep(random.uniform(0.1, 4)) # Simulate variable processing time

    return {
        "temperature": random.uniform(20, 30),
        "humidity": random.uniform(40, 60),
        "status": random.choice(["OK", "WARN"])
    }
###########################################################