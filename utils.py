"""
File that contains useful functions
"""


import os
import requests
import datetime
from dotenv import load_dotenv
from tapipy.tapis import Tapis
# from utils import get_streamflow_data, set_model_parameters, submit_subtask



# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env")         # Tapis credentials
load_dotenv("./Env/.env.public")  # Public env variables


# === CONFIGURATION ===
base_url = os.getenv('MINT_URL')


# === TASK SETUP ===
base_url = os.getenv('MINT_URL')
problem_statement = 'IDYnqZpBGvZpL4GPLRcg'
task = 'dwDiJ0dymXPd93kvlF9S'
sub_task = 'qwiUq7XqNK9bp6crSDj6'




def get_streamflow_data():
    """Fetch streamflow data from USGS API and extract the streamflow value."""
    url = "https://waterservices.usgs.gov/nwis/iv/?format=json&sites=15304000&siteStatus=all"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        
        # Navigate through the JSON structure
        time_series = data['value']['timeSeries']
        
        # Find the streamflow data (variable code 00060)
        for series in time_series:
            variable = series['variable']

            ### REVISAR EL JSON, POR QUE SE FILTRA Y QUE DEVUELVE Y SI PUEDE TRAERSE YA FILTRADO !!!

            if variable['variableCode'][0]['value'] == '00060':  # Streamflow code (Discharge, cubic feet per second)
                # Extract the latest value VERIFICAR QUE EXISTA INFORMACION EN ESE CAMPO Y COLOCAR VALOR POR DEFECTO (TERNARIOS) !!!
                latest_value = series['values'][0]['value'][0]['value']
                date_time = series['values'][0]['value'][0]['dateTime']
                site_name = series['sourceInfo']['siteName']
                variable_name = variable['variableName']
                unit = variable['unit']['unitCode']
                
                # Return Streamflow data
                return {
                    'site_name': site_name,
                    'variable_name': variable_name,
                    'value': latest_value,
                    'unit': unit,
                    'datetime': date_time
                }
        
        print("Streamflow data not found in the response")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error parsing JSON data: {e}")
        return None


def set_model_parameters(problem_statement_id, task_id, subtask_id, model_config, auth_token=None):
    """Set parameters for a specific subtask/model configuration."""
    # base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"
    endpoint = f"{base_url}/problemStatements/{problem_statement_id}/tasks/{task_id}/subtasks/{subtask_id}/parameters"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if auth_token:
        headers['Authorization'] = f'Bearer {auth_token}'
    
    try:
        print(f"Setting parameters at: {endpoint}")
        print(f"Payload: {model_config}")
        
        response = requests.post(endpoint, json=model_config, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        return result
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error setting parameters: {e}")
        print(f"Response status code: {response.status_code}")
        return None
    except Exception as e:
        print(f"Error setting parameters: {e}")
        return None


def submit_subtask(problem_statement_id, task_id, subtask_id, model_config, auth_token=None):
    """Submit a subtask for execution."""
    # base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"
    endpoint = f"{base_url}/problemStatements/{problem_statement_id}/tasks/{task_id}/subtasks/{subtask_id}/submit"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if auth_token:
        headers['Authorization'] = f'Bearer {auth_token}'
    
    try:
        print(f"Submitting subtask at: {endpoint}")
        print(f"Submit payload: ")
        
        response = requests.post(endpoint, json=model_config, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        print(f"Subtask submitted successfully:")
        return result
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error submitting subtask: {e}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except Exception as e:
        print(f"Error submitting subtask: {e}")
        return None


def get_next_hourly_filename():
    """
    Generates the name of file with next hour (H:00:00)
    """
    now = datetime.datetime.now()
    next_hour = now + datetime.timedelta(hours=1)
    return next_hour.strftime("metrics_data_%Y%m%d_%H0000.csv")


# TO-DO
def job_submission():
    """
    This function call USGS to get streamflow data to
    create a job for sumbission in Tapis, based on the flood
    detection
    """

    # Get streamflow data from USGS
    streamflow=0

    # GET the the streamflow data from the USGS
    streamflow_data = get_streamflow_data()

    # Check the result before read or modify
    if streamflow_data:
        # Convert from Cubic Feet to Cubic Meters, "35.315" = conversion factor
        if streamflow >= float(streamflow_data['value'])/35.315:
            logger.debug("Streamflow below threshold, waiting...")
            # time.sleep(3600) # Doing a sleep here will cause it to sleep 2 hours
            # continue
            # Just fallback and return true for the block be manage by metrics_receiver.py
        else:
            # Set the converted Amount as Current Streamflow
            streamflow = float(streamflow_data['value'])/35.315
            logger.info("Streamflow Value: %.2f m³/s", streamflow)

            # Create python Tapis client for user (for authentication)
            t = Tapis(base_url="https://portals.tapis.io",
                    username=os.getenv('userid'),
                    password=os.getenv('password'))

            # Get tokens now that you're initialized
            t.get_tokens()
            logger.info("Tapis client initialized: %s", str(t))

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
            # Submmit the job
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
        # If True, sleep for one hour
    #     return True

    # # If not, what to do????
    # return False


def job_submission_thread():
    flooding_job = threading.Thread(target=lambda: job_submission(), name="job_submission_thread")
    flooding_job.start()

    logger.info(f"💧 Starting job submission for Flooding...")
    return True
