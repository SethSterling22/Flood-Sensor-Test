"""
This program creates dummmy data
"""

import time
import random

###########################################################
def get_data():
    time.sleep(5)  # simula lectura o trabajo
    return {
        "temperature": random.uniform(20, 30),
        "humidity": random.uniform(40, 60),
        "status": random.choice(["OK", "WARN"])
    }
###########################################################
