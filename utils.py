import requests
base_url = "https://ensemble-manager.mint.tacc.utexas.edu/v1"

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