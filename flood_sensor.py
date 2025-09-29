from tapipy.tapis import Tapis
from dotenv import load_dotenv
import os
import logging
from utils import get_streamflow_data, set_model_parameters, submit_subtask
import time
import RPi.GPIO as GPIO


# === ENVIRONMENT  VARIABLES ===
load_dotenv(".env.public")  # Public env variables
load_dotenv(".env")         # Tapis credentials
load_dotenv(".env.config")  # Config env variables


sensor_pin = os.getenv('FLOOD_SENSOR')
GPIO.setmode(GPIO.BCM)
GPIO.setup(sensor_pin, GPIO.IN)


# === TASK SETUP ===
base_url = os.getenv('MINT_URL')
problem_statement = 'IDYnqZpBGvZpL4GPLRcg'
task = 'dwDiJ0dymXPd93kvlF9S'
sub_task = 'qwiUq7XqNK9bp6crSDj6'


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./Logs/flood_sensor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    # Get streamflow data from USGS
    streamflow=0
    logger.info("💧 Starting Flood Sensor monitoring...")
    
    try:
        while True:
            sensor_state = GPIO.input(sensor_pin)

            # HIGH = No water detected -> do nothing
            if sensor_state == GPIO.HIGH:
                time.sleep(0.5)
                streamflow = 0
            
            # If the sensor detects water, 
            else:
                # GET the the streamflow data from the USGS
                streamflow_data = get_streamflow_data()
                # Convert from Cubic Feet to Cubic Meters, "35.315" = conversion factor
                if streamflow >= float(streamflow_data['value'])/35.315:
                    logger.debug("Streamflow below threshold, waiting...")
                    time.sleep(3600)
                    continue
                else:
                    streamflow = float(streamflow_data['value'])/35.315
                    if streamflow_data:
                        logger.info(f"Streamflow Value: {streamflow:.2f} m³/s")
                        
                        # Create python Tapis client for user (for authentication)
                        t = Tapis(base_url="https://portals.tapis.io",
                                username=os.getenv('userid'),
                                password=os.getenv('password'))

                        # Get tokens now that you're initialized
                        t.get_tokens()
                        logger.info(f"Tapis client initialized: {t}")
                        
                        # Extract the access token for use with MINT API
                        auth_token = None
                        if hasattr(t, 'access_token') and t.access_token:
                            auth_token = t.access_token.access_token
                        
                        
                        # Configure the flood model with streamflow parameter
                        # You can use the actual streamflow value or a fixed value like 150
                        streamflow_value = str(int(streamflow))  # Convert to integer string
                        # Or use a fixed value: streamflow_value = "150"
                        
                        model_config = {
                            "model_id": "http://mint-model-catalog/v1.8.0/modelconfigurations/ec7e43eb-5b11-4ec9-84b0-81b527d8fbd5?username=mint@isi.edu",
                            "parameters": [
                                {
                                    "id": "https://w3id.org/okn/i/mint/57e7f177-77a8-44b1-9c00-dc50dc7eb7f7",
                                    "value": streamflow_value
                                }
                            ]
                        }
                        
                        logger.info("Setting Model Parameters")
                        params_result = set_model_parameters(problem_statement, task, sub_task, model_config, auth_token)
                        
                        if params_result:
                            logger.info("Submitting Subtask")
                            submit_result = submit_subtask(problem_statement, task, sub_task, model_config, auth_token)
                            
                            if submit_result:
                                logger.info("Flood model successfully configured and submitted!")
                                logger.info(f"Streamflow parameter: {streamflow_value}")
                                logger.info(f"Model ID: {model_config['model_id']}")
                            else:
                                logger.error("Failed to submit subtask")
                        else:
                            logger.error("Failed to set model parameters")
                    time.sleep(3600)

    except KeyboardInterrupt:
        logger.info("💧 Flood Sensor stopped by user.")
    finally:
        # Clean GPIO
        logger.info("Cleanup handled by gpiozero.")


if __name__ == '__main__':
    main()