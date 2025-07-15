import requests
from tapipy.tapis import Tapis
from dotenv import load_dotenv
import os
load_dotenv()

base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"
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
            if variable['variableCode'][0]['value'] == '00060':  # Streamflow code
                # Extract the latest value
                latest_value = series['values'][0]['value'][0]['value']
                date_time = series['values'][0]['value'][0]['dateTime']
                site_name = series['sourceInfo']['siteName']
                variable_name = variable['variableName']
                unit = variable['unit']['unitCode']
                
                print(f"Site: {site_name}")
                print(f"Variable: {variable_name}")
                print(f"Latest Value: {latest_value} {unit}")
                print(f"Date/Time: {date_time}")
                
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


def get_subtasks_list(problem_statement_id, task_id, auth_token=None):
    """Get the list of subtasks for debugging purposes."""
    
    endpoint = f"{base_url}/problemStatements/{problem_statement_id}/tasks/{task_id}/subtasks"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if auth_token:
        headers['Authorization'] = f'Bearer {auth_token}'
    
    try:
        print(f"Getting subtasks list from: {endpoint}")
        
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        
        subtasks_data = response.json()
        print(f"Available subtasks: {subtasks_data}")
        return subtasks_data
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error getting subtasks: {e}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except Exception as e:
        print(f"Error fetching subtasks list: {e}")
        return None


def set_model_parameters(problem_statement_id, task_id, subtask_id, model_config, auth_token=None):
    """Set parameters for a specific subtask/model configuration."""
    base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"
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
        print(f"Parameters set successfully: {result}")
        return result
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error setting parameters: {e}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except Exception as e:
        print(f"Error setting parameters: {e}")
        return None


def submit_subtask(problem_statement_id, task_id, subtask_id, model_config, auth_token=None):
    """Submit a subtask for execution."""
    base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"
    endpoint = f"{base_url}/problemStatements/{problem_statement_id}/tasks/{task_id}/subtasks/{subtask_id}/submit"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if auth_token:
        headers['Authorization'] = f'Bearer {auth_token}'
    
    try:
        print(f"Submitting subtask at: {endpoint}")
        print(f"Submit payload: {model_config}")
        
        response = requests.post(endpoint, json=model_config, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        print(f"Subtask submitted successfully: {result}")
        return result
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error submitting subtask: {e}")
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    except Exception as e:
        print(f"Error submitting subtask: {e}")
        return None




if __name__ == '__main__':
    # Get streamflow data from USGS
    streamflow_data = get_streamflow_data()
    
    if streamflow_data:
        print(f"\nStreamflow Value: {float(streamflow_data['value'])/35.315:.2f} m³/s")
        
        # Create python Tapis client for user (for authentication)
        t = Tapis(base_url="https://portals.tapis.io",
                username=os.getenv('userid'),
                password=os.getenv('password'))

        # Get tokens now that you're initialized
        t.get_tokens()
        print(f"\nTapis client initialized: {t}")
        
        # Extract the access token for use with MINT API
        auth_token = None
        if hasattr(t, 'access_token') and t.access_token:
            auth_token = t.access_token.access_token
            print(f"Using auth token: {auth_token[:20]}...")
        
        # First, let's get the list of available subtasks for debugging
        print(f"\n=== Getting Subtasks List ===")
        subtasks = get_subtasks_list(problem_statement, task, auth_token)
        
        # Configure the flood model with streamflow parameter
        # You can use the actual streamflow value or a fixed value like 150
        streamflow_value = str(int(float(streamflow_data['value'])))  # Convert to integer string
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
        
        print(f"\n=== Setting Model Parameters ===")
        params_result = set_model_parameters(problem_statement, task, sub_task, model_config, auth_token)
        
        if params_result:
            print(f"\n=== Submitting Subtask ===")
            submit_result = submit_subtask(problem_statement, task, sub_task, model_config, auth_token)
            
            if submit_result:
                print(f"\n✅ Flood model successfully configured and submitted!")
                print(f"   - Streamflow parameter: {streamflow_value}")
                print(f"   - Model ID: {model_config['model_id']}")
            else:
                print(f"\n❌ Failed to submit subtask")
        else:
            print(f"\n❌ Failed to set model parameters")
