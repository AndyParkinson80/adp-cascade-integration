# Standard Library - Core
import os
import sys
import json
import math
import tempfile
from pathlib import Path

# Standard Library - Time/Date
import time
from datetime import datetime, timedelta,time as dt_time

# Standard Library - Data Processing
import csv
from io import StringIO
from decimal import Decimal, ROUND_HALF_UP
import shutil

# Third-party - Data Processing
import pandas as pd
import requests

# Google Cloud Platform
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from google.cloud import secretmanager
from google.cloud import storage

debug = False

#base_path = Path(__file__).resolve().parent.parent             #Remove if program works 5 times without
current_folder = Path(__file__).resolve().parent

#----------------------------------------------------------------------------------# Set up

def find_run_type():
    current_time = datetime.now().time()

    if dt_time(0,0) <= current_time < dt_time(0,10):
        run_type = 1
    elif dt_time(0,30) <= current_time < dt_time(0,40):
        run_type = 2
    elif dt_time(1,0) <= current_time < dt_time(1,10):
        run_type = 3
    elif dt_time(3,0) <= current_time < dt_time(3,10):
        run_type = 1
    elif dt_time(3,30) <= current_time < dt_time(3,40):
        run_type = 4    
    elif dt_time(4,0) <= current_time < dt_time(4,10):
        run_type = 5
    else:
        run_type = 1

    return run_type

def create_folders(current_folder, structure=None, created_paths=None):
    if created_paths is None:
        created_paths = []
        
    if structure is None:
        structure = {
            "Data Store": {
                "Data - CAN": {
                    "002 - Security and Global": {},
                    "003 - Personal to Cascade": {},
                    "004 - Jobs to Cascade": {},
                    "005 - Absences to Cascade": {},
                    "006 - CascadeId to ADP": {},
                },
                "Data - USA": {
                    "002 - Security and Global": {},
                    "003 - Personal to Cascade": {},
                    "004 - Jobs to Cascade": {},
                    "005 - Absences to Cascade": {},
                    "006 - CascadeId to ADP": {},
                },
            }
        }

    for folder_name, subfolders in structure.items():
        # Create the folder
        folder_path = current_folder / folder_name
        os.makedirs(folder_path, exist_ok=True)
        
        # Add full path to the list
        created_paths.append(str(folder_path.resolve()))

        # Recursively create subfolders
        if isinstance(subfolders, dict) and subfolders:
            create_folders(folder_path, subfolders, created_paths)
    
    return created_paths

def delete_folders():
    base_path_delete = current_folder / "Data Store"
    folders_to_delete = ["Data - CAN", "Data - USA"]

    for folder_name in folders_to_delete:
        # Get the folder path
        folder_path = base_path_delete / folder_name
        
        # Delete the folder and its contents recursively if it exists
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

def google_auth():
    """
    Authenticate with Google Cloud and return credentials and project ID.
    
    First tries to use Application Default Credentials (ADC).
    If that fails, uses service account credentials from GOOGLE_CLOUD_SECRET environment variable.
    
    Returns:
        tuple: (credentials, project_id)
        
    Raises:
        Exception: If both authentication methods fail
    """
    try:
        # Try Application Default Credentials first
        credentials, project_id = default()
        print("Successfully authenticated using Application Default Credentials")
        return credentials, project_id
         
    except DefaultCredentialsError:
        print("Application Default Credentials not available, trying service account...")
        
        # Try service account from environment variable
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if not secret_json:
            raise Exception("GOOGLE_CLOUD_SECRET environment variable not found")
        
        try:
            # Parse the JSON credentials
            service_account_info = json.loads(secret_json)
            
            # Create credentials from service account info
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info
            )
            
            # Extract project ID from service account info
            project_id = service_account_info.get('project_id')
            if not project_id:
                raise Exception("project_id not found in service account credentials")
            
            print("Successfully authenticated using service account credentials")
            return credentials, project_id
            
        except json.JSONDecodeError:
            raise Exception("Invalid JSON in GOOGLE_CLOUD_SECRET environment variable")
        except Exception as e:
            raise Exception(f"Failed to create service account credentials: {str(e)}")
    
    except Exception as e:
        raise Exception(f"Authentication failed: {str(e)}")

def debug_check(debug):
    if debug:
        folder_paths = create_folders(current_folder)
        print("Created folders:")
        for path in folder_paths:
            print(path)
        extended_update = False                                          
        Data_export = True
    else:
        extended_update = True                                                            
        Data_export = False
        if Data_export:
            folder_paths = create_folders(current_folder)
            print("Created folders:")
            #for path in folder_paths:
            #    print(path)
        
    return extended_update,Data_export

def data_store_location(country):
    if country == "usa":                                                                             
        data_store = os.path.join(current_folder,"Data Store","Data - USA")
        return data_store
    else:
        data_store = os.path.join(current_folder,"Data Store","Data - CAN")
        return data_store
    
def load_keys(country):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Gathering Security Information (" + time_now + ")")                                                                    #loads keys from secure external file
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Loading Security Keys (" + time_now + ")")
        
        def get_secrets(secret_id):
            def access_secret_version(project_Id, secret_id, version_id="latest"):

                client = secretmanager.SecretManagerServiceClient(credentials=creds)
                name = f"projects/{project_Id}/secrets/{secret_id}/versions/{version_id}"

                response = client.access_secret_version(request={"name": name})
                payload = response.payload.data.decode("UTF-8")

                return payload

            version_id = "latest" 

            secret = access_secret_version(project_Id, secret_id, version_id)
            #print(f"Secret value: {secret}")

            return secret

        client_id = get_secrets(f"ADP-{country}-client-id")
        client_secret = get_secrets(f"ADP-{country}-client-secret")
        country_hierarchy_USA = get_secrets(f"country_Hierarchy_USA")
        country_hierarchy_CAN = get_secrets("country_Hierarchy_CAN")
        strings_to_exclude = get_secrets("strings_to_exclude")
        cascade_API_id = get_secrets("cascade_API_id")
        keyfile = get_secrets(f"{country}_cert_key")
        certfile = get_secrets(f"{country}_cert_pem")
        
        return client_id,client_secret,strings_to_exclude,country_hierarchy_USA,country_hierarchy_CAN,cascade_API_id,keyfile,certfile

def load_ssl(certfile_content, keyfile_content):
    """
    Create temporary files for the certificate and keyfile contents.
    
    Args:
        certfile_content (str): The content of the certificate file.
        keyfile_content (str): The content of the key file.
    
    Returns:
        tuple: Paths to the temporary certificate and key files.
    """
    # Create temporary files for certfile and keyfile
    temp_certfile = tempfile.NamedTemporaryFile(delete=False)
    temp_keyfile = tempfile.NamedTemporaryFile(delete=False)

    try:
        # Write the contents into the temporary files
        temp_certfile.write(certfile_content.encode('utf-8'))
        temp_keyfile.write(keyfile_content.encode('utf-8'))
        temp_certfile.close()
        temp_keyfile.close()

        # Return the paths of the temporary files
        return temp_certfile.name, temp_keyfile.name
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e

def security(client_id, client_secret, cascade_API_id,temp_certfile,temp_keyfile):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Creating Credentials (" + time_now + ")")
        
        certfile = temp_certfile
        keyfile = temp_keyfile

        def adp_bearer():
            adp_token_url = 'https://accounts.adp.com/auth/oauth/v2/token'                                                                                          

            adp_token_data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret
            }
            adp_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            adp_token_response = requests.post(adp_token_url, cert=(certfile, keyfile), verify=True, data=adp_token_data, headers=adp_headers)

            if adp_token_response.status_code == 200:
                access_token = adp_token_response.json()['access_token']

            return access_token

        def cascade_bearer ():
            cascade_token_url='https://api.iris.co.uk/oauth2/v1/token'
            
            cascade_token_data = {
                'grant_type':'client_credentials',
                            }
            cascade_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                "Authorization": f'Basic:{cascade_API_id}'
                    }

            cascade_token_response = requests.post(cascade_token_url, data=cascade_token_data, headers=cascade_headers)

            #checks the api response and extracts the bearer token
            if cascade_token_response.status_code == 200:
                cascade_token = cascade_token_response.json()['access_token']
            
            return cascade_token
        
        access_token = adp_bearer()
        cascade_token = cascade_bearer()

        return keyfile,certfile,access_token,cascade_token

#----------------------------------------------------------------------------------# Global Data Calls

def GET_workers_adp():
    api_url = 'https://api.adp.com/hr/v2/workers'

    def max_staff_rounded_up_to_100():
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Retrieving Data from ADP Workforce Now (" + time_now + ")")
        api_headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept':"application/json;masked=false",  
            }
        api_count_params = {
                "count": "true",
            }

        api_count_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers, params=api_count_params)                 #data request. Find number of records and uses this to find the pages needed
        response_data = api_count_response.json()
        total_number = response_data.get("meta", {}).get("totalNumber", 0)
        rounded_total_number = math.ceil(total_number / 100) * 100
        return rounded_total_number
    
    API_pagination_calls = max_staff_rounded_up_to_100()

    adp_responses = []                                                                                                                              # Initialize an empty list to store API responses. This will also store the outputted data
    adp_terminated = []

    def make_api_request_active(skip_param):                                                                                                        # Function to make an API request with skip_param and append the response to all_responses

        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }

        api_params = {
            "$filter": "workers/workAssignments/assignmentStatus/statusCode/codeValue eq 'A'",
            "$top": 100,
            "$skip": skip_param
            }

        api_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers, params=api_params)
        time.sleep(0.6)

        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            adp_responses.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    def make_api_request_leave():
        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }

        api_params = {
            "$filter": "workers/workAssignments/assignmentStatus/statusCode/codeValue eq 'L'",
            }

        api_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers, params=api_params)
        time.sleep(0.6)

        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            adp_responses.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    def make_api_request_terminated(skip_param):                                                                                                        # Function to make an API request with skip_param and append the response to all_responses

        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }

        api_params = {
            "$filter": "workers/workAssignments/assignmentStatus/statusCode/codeValue eq 'T'",
            "$top": 100,
            "$skip": skip_param
            }

        api_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers, params=api_params)
        time.sleep(0.6)

        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            adp_terminated.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    total_records = 0
    skip_param = 0

    while True:
        make_api_request_active(skip_param)
        skip_param += 100
        total_records += 100 
        if total_records >= API_pagination_calls:  
            break
    
    total_records = 0
    skip_param = 0

    while True:
        make_api_request_terminated(skip_param)
        skip_param += 100
        total_records += 100 
        if total_records >= API_pagination_calls:  
            break

    make_api_request_leave()
    
    def filter_records(record):
        return [
            worker for worker in record['workers']
            if worker['workerID']['idValue'] not in strings_to_exclude
        ]

    # Filter both datasets
    adp_responses = [dict(workers=filter_records(record)) for record in adp_responses]
    adp_terminated = [dict(workers=filter_records(record)) for record in adp_terminated]

    # Combine workers separately
    combined_workers_responses = []
    for item in adp_responses:
        combined_workers_responses.extend(item["workers"])

    combined_workers_terminated = []
    for item in adp_terminated:
        combined_workers_terminated.extend(item["workers"])

    filtered_workers_terminated = []
    
    for worker in combined_workers_terminated:
        try:
            # Extract termination date from the worker record
            termination_date_str = worker.get('workerDates', {}).get('terminationDate')
            
            if termination_date_str:
                # Parse the termination date (assuming YYYY-MM-DD format)
                termination_date = datetime.strptime(termination_date_str, '%Y-%m-%d')
                
                # Check if termination date is within the last 6 months
                if termination_date >= x_months_ago:
                    filtered_workers_terminated.append(worker)
                    
        except (ValueError, KeyError, TypeError) as e:
            # Skip records with invalid or missing termination dates
            print(f"Skipping record due to date parsing error: {e}")
            continue
    
    combined_data_responses = [{
        "workers": combined_workers_responses,
        "meta": None,
        "confirmMessage": None
    }]

    combined_data_terminated = [{
        "workers": filtered_workers_terminated,
        "meta": None,
        "confirmMessage": None
    }]

    if Data_export:
        file_path = os.path.join(data_store,"002 - Security and Global","001 - ADP (Data Out).json")
        with open(file_path, "w") as outfile:
            json.dump(combined_data_responses, outfile, indent=4)
        file_path = os.path.join(data_store,"002 - Security and Global","001 - ADP (Data Out - Terminations).json")
        with open(file_path, "w") as outfile:
            json.dump(combined_data_terminated, outfile, indent=4)
    return combined_data_responses,combined_data_terminated

def GET_workers_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Retrieving current Personal Data from Cascade HR (" + time_now + ")")

    cascade_responses_initial = []

    def api_count():
        api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
        api_headers = {
            'Authorization': f'Bearer {cascade_token}',
        }
        if extended_update == True:                         
            api_params = {
            }
        else:
            api_params = {
                "$filter": "EmploymentLeftDate eq null",
            } 
        
        api_count_response = requests.get(api_url, headers=api_headers, params=api_params)
        response_data = api_count_response.json()

        total_number = response_data['@odata.count']
        rounded_total_number = math.ceil(total_number / 200) * 200
        return rounded_total_number
    
    def make_api_request(skip_param):
        api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
        api_headers = {
            'Authorization': f'Bearer {cascade_token}',
        }
        
        if extended_update == True:                         
            api_params = {
                "$top": 200,
                "$skip": skip_param
            }
        else:
            api_params = {
                "$filter": "EmploymentLeftDate eq null",
                "$top": 200,
                "$skip": skip_param
            }                

        api_response = requests.get(api_url, headers=api_headers, params=api_params)
        time.sleep(0.6)
        if api_response.status_code == 200:
            #checks the response and writes the response to a variable
            json_data = api_response.json()

            # Append the response to all_responses
            cascade_responses_initial.append(json_data)

            # Check for a 204 status code and break the loop
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    max_records = api_count()

    total_records = 0
    skip_param = 0

    while True:
        make_api_request(skip_param)
        #maximum returned records for WFN is 100. This small loop alters the $skip variable and requests the 'next' 100
        # Increment skip_param by 100 for the next request
        skip_param += 200
        total_records += 200  # Keep track of the total number of records retrieved
        
        # Break the loop when there are no more records to retrieve
        if total_records >= max_records:  
            break

        time.sleep(0.6)

    # Combine all the "workers" arrays into a single array
    combined_value = []
    for item in cascade_responses_initial:
        combined_value.extend(item["value"])

    # Create a new dictionary with the combined workers
    combined_data = [{
        "value": combined_value,
        "meta": None,
        "confirmMessage": None
    }]

    filtered_data = []

    for record_set in combined_data:
        for record in record_set.get('value', []):
            if record.get('DisplayId') is not None:
                filtered_data.append(record)

    if Data_export:
        file_path = os.path.join(data_store,"002 - Security and Global","001 - Cascade Raw Out.json")
        with open(file_path, "w") as outfile:
            json.dump(filtered_data, outfile, indent=4)
    

    return filtered_data

def GET_hierarchy_nodes(country):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Retrieving Job Hierarchy Nodes (" + time_now + ")")

    if country == "usa":
        H_top_level = country_hierarchy_USA
    elif country == "can":
        H_top_level = country_hierarchy_CAN

    api_url = 'https://api.iris.co.uk/hr/v2/hierarchy'
    api_headers =   {
        'Authorization': f'Bearer {cascade_token}'
                    }
    
    def get_hierarchy_nodes(hierarchy_ids):
        hierarchy_nodes = []
        hierarchy_id_nodes = []

        for h_id in hierarchy_ids:
            api_params = {
                "$filter": f"parentId eq '{h_id}' and disabled eq false"
            }

            for attempt in range(2):  # Attempt up to 2 times
                response = requests.get(api_url, params=api_params, headers=api_headers)
                time.sleep(0.6)  # Always sleep between requests

                if response.status_code == 200:
                    data = response.json()
                    for record in data.get('value', []):
                        hierarchy_nodes.append(record)
                        hierarchy_id_nodes.append(record['Id'])
                    break  # Exit retry loop on success
                elif attempt == 0:
                    print(f"Request failed for parentId {h_id}, retrying in 1 second...")
                    time.sleep(3)
                else:
                    print(f"Failed to retrieve data for parentId {h_id}: {response.status_code}")

        return hierarchy_nodes, hierarchy_id_nodes
    # Initial call to get top-level nodes
    initial_params = {
        "$filter": f"Id eq '{H_top_level}'"
    }

    response = requests.get(api_url, params=initial_params, headers=api_headers)
    if response.status_code == 200:
        data = response.json()
        hierarchy_nodes = []
        hierarchy_id_nodes = []
        
        # Extract the initial records and Id values
        for record in data.get('value', []):
            hierarchy_nodes.append(record)
            hierarchy_id_nodes.append(record['Id'])

        # Recursive call to get all descendant nodes
        while hierarchy_id_nodes:
            new_nodes, new_id_nodes = get_hierarchy_nodes(hierarchy_id_nodes)
            hierarchy_nodes.extend(new_nodes)
            hierarchy_id_nodes = new_id_nodes
    else:
        print(f"Failed to retrieve data: {response.status_code}")

    if Data_export:
        file_path = os.path.join(data_store,"002 - Security and Global","002 - Hierarchy Nodes.json")
        with open(file_path, "w") as outfile:
            json.dump(hierarchy_nodes, outfile, indent=4)
    
    return hierarchy_nodes

def ID_generator(country,adp_responses,cascade_responses,hierarchy_nodes):

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Creating an ID library (" + time_now + ")")

    ID_library = []

    for record in adp_responses:
        workers = record["workers"]
        for worker in workers:

            active_job_position = None

            work_assignments = worker.get("workAssignments", [{}])
            for index, assignment in enumerate(work_assignments):
                if assignment.get("primaryIndicator", True):
                    active_job_position = index
                    continue

            id_value = worker["associateOID"]
            contServiceADP = worker["workAssignments"][0]["hireDate"]
            date_obj = datetime.strptime(contServiceADP, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%Y-%m-%dT00:00:00Z")


            ADP_identifier = worker["workAssignments"][active_job_position]["positionID"]

            if country == "usa":
                job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][1]["nameCode"]["codeValue"]
                file_path = os.path.join(current_folder,"Hierarchy.xlsx")
                df = pd.read_excel(file_path,sheet_name="usa Conversion")
            
                H1 = df.where(pd.notna(df), None).to_dict(orient='records')
                for item in H1:
                    item["ADP Code"] = str(item["ADP Code"])
                    item["Cascade Code"] = str(item["Cascade Code"])

                if Data_export:
                    file_path = os.path.join(data_store,"002 - Security and Global","002 - USA Hierarchy.json")
                    with open(file_path, "w") as outfile:
                        json.dump(H1, outfile, indent=4)
                
                hierarchy = None
                if hierarchy is None:
                    for item in H1:
                        if item["ADP Code"] == job_code:
                            hierarchy = item["Cascade Code"]
                            break

                for node in hierarchy_nodes:
                    if node.get("SourceSystemId") == hierarchy:
                        hierarchy_id = node.get("Id",None)

                LM_kzo = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID")
                # Find matching display_id (case-insensitive)
                CascadeID = None

                for entry in cascade_responses:
                    # Safely get the NationalInsuranceNumber or skip if missing
                    if entry.get("NationalInsuranceNumber") == ADP_identifier:
                        CascadeID = entry.get("DisplayId")

                        if CascadeID is None:
                            Cascade_full = None
                        else:
                            Cascade_full = entry.get("Id")
                            contServiceCascade = entry.get("ContinuousServiceDate")
                        break  # Exit loop once a match is found

                if CascadeID is None:
                    date = formatted_date
                    contServiceCascade = None
                else:
                    date = min(contServiceCascade,formatted_date)

                # Create a dictionary for the transformed data
                transformed_record = {
                    "AOID": id_value,
                    "CascadeId": CascadeID if CascadeID is not None else None,
                    "Cascade_full": Cascade_full if CascadeID is not None else None, 
                    "ADP_number": ADP_identifier,
                    "ADP_line_manager": LM_kzo,
                    "Job_position": active_job_position,
                    "Hierarchy": hierarchy_id,
                    "Cascade Start": contServiceCascade,
                    "ADP Start": formatted_date,
                    "contServiceDate": date,
                }
                ID_library.append(transformed_record)

            elif country == "can":
                job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][0]["nameCode"]["codeValue"]
                job_name = worker["workAssignments"][active_job_position]["jobTitle"]
                file_path = os.path.join(current_folder,"Hierarchy.xlsx")
                df = pd.read_excel(file_path,sheet_name="can Conversion")
                
                df_1 = df.dropna()

                df_2 = df[df.isna().any(axis=1)]
                df_2 = df_2.drop(columns=['job_name'])

                H1 = df_1.where(pd.notna(df_1), None).to_dict(orient='records')
                for item in H1:
                    item["job_code"] = str(item["job_code"])
                    item["hierarchy"] = str(item["hierarchy"])
            
                H2 = df_2.where(pd.notna(df_2), None).to_dict(orient='records')
                for item in H2:
                    item["job_code"] = str(item["job_code"])
                    item["hierarchy"] = str(item["hierarchy"])

                if Data_export:
                    file_path = os.path.join(data_store,"002 - Security and Global","002 - Can Hierarchy (both).json")
                    with open(file_path, "w") as outfile:
                        json.dump(H1, outfile, indent=4)
                    file_path = os.path.join(data_store,"002 - Security and Global","002 - Can Hierarchy (single).json")
                    with open(file_path, "w") as outfile:
                        json.dump(H2, outfile, indent=4)
                
                hierarchy = None
                for item in H1:
                    if item["job_code"] == job_code and item["job_name"] in job_name:
                        hierarchy = item["hierarchy"]
                        break
                if hierarchy is None:
                    for item in H2:
                        if item["job_code"] == job_code:
                            hierarchy = item["hierarchy"]
                            break            

                LM_kzo = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID")
                # Find matching display_id (case-insensitive)
                CascadeID = None

                for entry in cascade_responses:
                    if entry["NationalInsuranceNumber"] == ADP_identifier:
                        CascadeID = entry["DisplayId"]
                        Cascade_full = entry["Id"]
                        contServiceCascade = entry["ContinuousServiceDate"]
                        break
                
                if CascadeID is None:
                    date = formatted_date
                    contServiceCascade = None
                else:
                    date = min(contServiceCascade,formatted_date)

                        
                # Create a dictionary for the transformed data
                transformed_record = {
                    "AOID": id_value,
                    "CascadeId": CascadeID if CascadeID is not None else None,
                    "Cascade_full": Cascade_full if CascadeID is not None else None, 
                    "ADP_number": ADP_identifier,
                    "ADP_line_manager": LM_kzo,
                    "Job_position": active_job_position,
                    "Hierarchy": hierarchy,
                    "Job Code": job_code,
                    "job Name": job_name,
                    "contServiceDate": date,
                }
                ID_library.append(transformed_record)

        if Data_export:
            file_path = os.path.join(data_store,"002 - Security and Global","003 - ID_library.json")
            with open(file_path, "w") as outfile:
                json.dump(ID_library, outfile, indent=4)
        
        return ID_library
    
#----------------------------------------------------------------------------------# Cascade to ADP

def whats_in_ADP(adp_responses, ID_library, country):
    ct_POST_cascade_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Finding Id's that are missing on Cascade (" + ct_POST_cascade_id + ")")
    ID_responses = []

    for record in adp_responses:
        workers = record["workers"]
        for worker in workers:
            work_assignments = worker.get("workAssignments", [{}])
            for index, assignment in enumerate(work_assignments):
                if assignment.get("primaryIndicator", True):
                    active_job_position = index
                    continue
            ADP_number = worker["workAssignments"][active_job_position]["positionID"]
            if country == "usa":
                Cascade = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
            if country == "can":
                Cascade = worker["customFieldGroup"]["stringFields"][0].get("stringValue", "")

            cascade_exists_in_library = False

            for record in ID_library:
                if record["CascadeId"] == Cascade:
                    cascade_exists_in_library = True
                    break             
            
            if not cascade_exists_in_library:              
                for record in ID_library:
                    if record["ADP_number"]==ADP_number:
                        Cascade = record["CascadeId"]
                        AOID = record["AOID"]
                
                transformed_record = {
                    "AOID": AOID,
                    "Cascade": Cascade
                    #"Cascade": Cascade if Cascade is not None else ""              #Use this to strip CascadeId's out of Canadian Records
                    }

                ID_responses.append(transformed_record)
    
    if Data_export:
        file_path = os.path.join(data_store,"006 - CascadeId to ADP","003 - IDs_updating.json")
        with open(file_path, "w") as outfile:
            json.dump(ID_responses, outfile, indent=4)
    
    return ID_responses

def upload_cascade_Ids_to_ADP(CascadeId_to_upload,country):
    ct_POST_cascade_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Updating Cascade ID's on WFN (" + ct_POST_cascade_id + ")")

    api_url = 'https://api.adp.com/events/hr/v1/worker.person.custom-field.string.change'
    
    for entry in CascadeId_to_upload:
        AOID = entry['AOID']
        if country == "usa":
            ItemID = '9200019333951_24129'         
        if country == "can":
            ItemID = '9200820366120_1'
        cascadeId = entry['Cascade']
        
        schema = {
                "events": [
                    {
                        "data": {
                            "eventContext": {
                                "worker": {
                                    "associateOID": AOID,
                                    "person": {
                                        "customFieldGroup": {
                                            "stringField": {
                                                "itemID": ItemID
                                            }
                                        }
                                    }
                                }
                            },
                            "transform": {
                                "worker": {
                                    "person": {
                                        "customFieldGroup": {
                                            "stringField": {
                                                "stringValue": cascadeId
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }

        data_to_write = json.dumps(schema, indent=4)


        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': "application/json",
        }            
        req = requests.post(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers, data=data_to_write)
        
        if req.status_code ==200:
            print ("        "+f'Data uploaded for CascadeId: {cascadeId}')
        else:
            print("        "+f'Response Code: {req.status_code}')
    

    return ct_POST_cascade_id
    
#----------------------------------------------------------------------------------# Delete Absences

def load_from_bucket(variable):
    client = storage.Client(credentials=creds, project=project_Id)
    bucket = client.bucket("event_list_objects")
    blob = bucket.blob(f"{variable}.json")

    data = json.loads(blob.download_as_text())
    string_list = data["strings"]

    return string_list

def create_absences_reasons():
    file_path = os.path.join(current_folder,"Hierarchy.xlsx")                                               #creates a dictionary to link ADP and cascade absence codes
    df = pd.read_excel(file_path,sheet_name=f"{c} Absences")
    
    if c == "usa":
        absence_reasons = df.where(pd.notna(df), None).to_dict(orient='records')
        for item in absence_reasons:
            item["policy"] = str(item["policy"])
            item["earningType"] = str(item["earningType"])
            item["narrative"] = str(item["narrative"])
            item["cascadeAbsenceId"] = str(item["cascadeAbsenceId"])
    elif c == "can":
        absence_reasons = df.where(pd.notna(df), None).to_dict(orient='records')
        for item in absence_reasons:
            item["Name"] = str(item["Name"])
            item["Id"] = str(item["Id"])
    
    if Data_export:
        file_path = os.path.join(data_store,"005 - Absences to Cascade",f"001 - {c} absence reasons.json")
        with open(file_path, "w") as outfile:
            json.dump(absence_reasons, outfile, indent=4)
    
    return absence_reasons

def get_cascade_id(CascadeId,ID_library):
    for record in ID_library:
        if record["CascadeId"] == CascadeId:
            Cascade_full = record["Cascade_full"]
            AOID = record["AOID"]
            break
    
    return Cascade_full,AOID

def get_absences_adp(AOID):

    api_url = "https://api.adp.com/time/v2/workers/" + AOID + "/time-off-details/time-off-requests"

    api_headers = {
        'Authorization': f'Bearer {access_token}',
    }

    api_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers)
    adp_response = api_response.json()

    if Data_export:
        file_path = os.path.join(data_store,"005 - Absences to Cascade","002 - ADP Raw absence response (Individual).json")
        with open(file_path, "w") as json_file:
            json.dump(adp_response, json_file, indent=4)
    return adp_response

def convert_ADP_absences_to_cascade_format(adp_response,absence_reasons,Cascade_full,AOID,ninety_days_ago):
    data = adp_response
    output = {"Pending": [], "Approved": [], "Cancelled": []}

    categories = len(data["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"])

    for a in range(categories):
        absence_type = data["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"][a]["requestStatus"]["labelName"]
        category = data["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"][a]
        records = len(category["requests"])

        for x in range(records):
            new_record = []
            main = category["requests"][x]
            absencePolicy = main["paidTimeOffEntries"][0]["paidTimeOffPolicy"].get("labelName", "")
            if c == "usa":
                absenceEarningType = main["paidTimeOffEntries"][0]["earningType"].get("labelName", "")
                for absence in absence_reasons:
                    if absence['policy'] == absencePolicy and absence['earningType'] == absenceEarningType:
                        AbsenceReasonId = absence['cascadeAbsenceId']
            if c == "can":
                for absence in absence_reasons:
                    if absence['Name'] == absencePolicy:
                        AbsenceReasonId = absence['Id']

            last_record = len(main["paidTimeOffEntries"])

            start_date_string = main["paidTimeOffEntries"][0]["timePeriod"]["startDateTime"]
            end_date_string = main["paidTimeOffEntries"][last_record-1]["timePeriod"]["endDateTime"]

            new_record = {
                f"Record Position {x}": {
                    "Section": a,
                    "Record": x,
                    "isFromSync": False,
                    "trackingId": None,
                    "EmployeeId": Cascade_full,
                    "AbsenceReasonId": AbsenceReasonId,
                    "Label name": absenceEarningType if c == "usa" else None,
                    "Narrative": None,
                    "StartDate": start_date_string,
                    "EndDate": end_date_string,
                }
            }

            if absence_type == "Pending":
                output["Pending"].append(new_record)
            elif absence_type == "Approved":
                output["Approved"].append(new_record)
            elif absence_type == "Cancelled":
                output["Cancelled"].append(new_record)

    final_output = {
        "AOID": AOID,
        "Absence_Data": output
    }

    approved_absences = final_output["Absence_Data"]["Approved"]

    extracted_records = []

    for record in approved_absences:
        for key, details in record.items():
            extracted_record = {
                "Section": details["Section"],
                "Record": details["Record"],
                "isFromSync": details["isFromSync"],
                "trackingId": details["trackingId"],
                "EmployeeId": details["EmployeeId"],
                "AbsenceReasonId": details["AbsenceReasonId"],
                "Narrative": details["Narrative"],
                "StartDate": details["StartDate"],
                "EndDate": details["EndDate"]
            }
            extracted_records.append(extracted_record)

    filtered_extracted_records = [record for record in extracted_records if datetime.strptime(record["StartDate"], "%Y-%m-%d") >= ninety_days_ago]


    if Data_export:
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "003 - ADP absences - categorised.json")
        with open(file_path, "w") as json_file:
            json.dump(final_output, json_file, indent=4)
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "003a - ADP absences - Approved.json")
        with open(file_path, "w") as json_file:
            json.dump(filtered_extracted_records, json_file, indent=4)

    return filtered_extracted_records

def cascade_absences(Cascade_full,absences_from):

    cascade_responses = []

    api_url = 'https://api.iris.co.uk/hr/v2/attendance/absences'
    api_headers = {
        'Authorization': f'Bearer {cascade_token}',
    }
    api_params = {
        #"$filter": "EmployeeId eq '"+Cascade_full+"'",                                              #filters to all absences...
        "$filter": "EmployeeId eq '"+Cascade_full+"' and startDate ge "+absences_from,             #add this to filter to the last 90 days
    }
    api_response = requests.get(api_url, headers=api_headers, params=api_params)
    
    if api_response.status_code == 200:
        #checks the response and writes the response to a variable
        json_data = api_response.json()

        # Append the response to all_responses
        cascade_responses.append(json_data)

        # Check for a 204 status code and break the loop
        if api_response.status_code == 204:
            return True
    elif api_response.status_code == 204:
        return True
    else:
        print(f"Failed to retrieve data from API. Status code: {api_response.status_code}")

    
    def convert_datetime_to_date(datetime_str):
        dt_object = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt_object.strftime("%Y-%m-%d")

    combined_data = [entry for response in cascade_responses for entry in response.get('value', [])]

    updated_json_data = [
        {
            "isFromSync": False,
            "trackingId": None,
            "id": entry["Id"],
            "EmployeeId": entry["EmployeeId"],
            "AbsenceReasonId": entry["AbsenceReasonId"],
            "Narrative": entry["Narrative"],
            "StartDate": convert_datetime_to_date(entry["StartDate"]),
            "EndDate": convert_datetime_to_date(entry["EndDate"]),
        }
        for entry in combined_data
    ]

    current_absence_id_cascade = [entry["id"] for entry in updated_json_data]

    if Data_export:
        file_path = os.path.join(data_store,"005 - Absences to Cascade","005 - Cascadecurrent.json")
        with open(file_path, "w") as outfile:
            json.dump( updated_json_data, outfile, indent=4)
        file_path = os.path.join(data_store,"005 - Absences to Cascade","005a - Cascadecurrent_Id.json")
        with open(file_path, "w") as outfile:
            json.dump(current_absence_id_cascade, outfile, indent=4)
    
    return  updated_json_data,current_absence_id_cascade

def combine_json_files_for_POST(current_absence_id_cascade,adp_current,cascade_current):
    update_records = []
    update_ids = []
    unchanged_records = []
    unchanged_ids = []
    processed_records = set()
    Update_transformed = []

    for adp_data in adp_current:
        for cascade_record in cascade_current:
            conditions_met = (
                adp_data['StartDate'] == cascade_record['StartDate'],
                adp_data['EndDate'] == cascade_record['EndDate'],
                adp_data['AbsenceReasonId'] == cascade_record['AbsenceReasonId']
            )
            if all(conditions_met):  # All three are the same - No changes needed
                unchanged_records.append(adp_data)
                processed_records.add((adp_data['EmployeeId'], adp_data['StartDate'], adp_data['EndDate']))
                unchanged_ids.append(cascade_record['id'])
                break
            elif sum(conditions_met) >= 2:  # One or more fields need changing
                update_records.append(adp_data)
                processed_records.add((adp_data['EmployeeId'], adp_data['StartDate'], adp_data['EndDate']))
                update_ids.append(cascade_record['id'])
                break

    # All other records that are not processed need to be added as new records
    new_records = [record for record in adp_current if (record['EmployeeId'], record['StartDate'], record['EndDate']) not in processed_records]

    for index, record in enumerate(update_records):
        transformed_record = {
            "AbsenceId": update_ids[index],
            "Payload": {
                "Narrative": record["Narrative"],
                "StartDate": record["StartDate"],
                "EndDate": record["EndDate"],
                "Id": record["AbsenceReasonId"]
            }
        }
        Update_transformed.append(transformed_record)

    current_absence_set = set(current_absence_id_cascade)
    unchanged_set = set(unchanged_ids)
    update_set = set(update_ids)
    result_set = current_absence_set - (unchanged_set | update_set)
    delete_ids = list(result_set)

    if Data_export:
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "006 - Unchanged.json")
        with open(file_path, "w") as outfile:
            json.dump(unchanged_records, outfile, indent=4)
        
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "006a - Unchanged id.json")
        with open(file_path, "w") as outfile:
            json.dump(unchanged_ids, outfile, indent=4)
        
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "007 - Update.json")
        with open(file_path, "w") as outfile:
            json.dump(update_records, outfile, indent=4)

        file_path = os.path.join(data_store, "005 - Absences to Cascade", "007a - Update (reordered).json")
        with open(file_path, "w") as outfile:
            json.dump(Update_transformed, outfile, indent=4)
        
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "007b - Update id.json")
        with open(file_path, "w") as outfile:
            json.dump(update_ids, outfile, indent=4)
        
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "008 - New.json")
        with open(file_path, "w") as outfile:
            json.dump(new_records, outfile, indent=4)
        
        file_path = os.path.join(data_store, "005 - Absences to Cascade", "009 - Delete_Id.json")
        with open(file_path, "w") as outfile:
            json.dump(delete_ids, outfile, indent=4)

    return new_records, Update_transformed, delete_ids, update_ids

def POST(new_records,adp_response,Cascade_full):
    def absence_days_from_adp(trackingID):        #subsume into absences from adp
        data = adp_response

        output2=[]
        uploads = []

        main = data["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"][section]["requests"][records]
        records_to_upload = main["paidTimeOffEntries"]


        for i, entry in enumerate(records_to_upload):
            entry_status_label_name = entry.get("entryStatus", {}).get("labelName")
            if entry_status_label_name is not None and entry_status_label_name == "Approved":
                uploads.append(i)

        absenceId = trackingID               
            
        for b in uploads:
            address = main["paidTimeOffEntries"][b]
            
            start = address["timePeriod"]["startDateTime"]
            start_time = address["startTime"]
            if c == "usa":
                hours = address["totalQuantity"]["valueNumber"]
                days = hours/8
                minutes = hours * 60
            elif c == "can":
                time_period = address["totalQuantity"]["unitTimeCode"]
                if time_period == "day":
                    days = address["totalQuantity"]["valueNumber"]
                    hours = days * 8
                else:
                    hours = address["totalQuantity"]["valueNumber"]
                    days = hours / 8
                
                minutes = hours * 60

            start_hour = int(start_time.split(":")[0])
            if start_hour > 12 and hours < 7:
                DayPart =  "pm"
            elif start_hour <= 12 and hours < 7:
                DayPart =  "am"
            else:
                DayPart = "AllDay"

            new_record = {
                "AbsenceId": absenceId,
                "EmployeeId": Cascade_full,
                "Date": start,
                "DurationDays": str(float(days)) ,
                "DurationMinutes": str(int(minutes)),
                "DayPart": DayPart,
            }

            print (new_record)

            api_url = 'https://api.iris.co.uk/hr/v2/attendance/absencedays'

            headers = {
                'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
                'Authorization': f'Bearer {cascade_token}',
                'Content-Type':'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
                    }
                    
            response = requests.post(api_url, headers=headers, json=new_record)
            time.sleep(1)  
                        
        if Data_export:
            file_path = os.path.join(data_store,"005 - Absences to Cascade", "absence_days.json")
            with open(file_path, "w") as json_file:
                json.dump(output2, json_file, indent=4)

    records_to_add = new_records
    output=[]

    if not records_to_add:
        print("                No records to add")
    else:
        for record in records_to_add:
            section = record["Section"]
            records = record["Record"]

            new_record = {
                "EmployeeId": Cascade_full,
                "AbsenceReasonId": record["AbsenceReasonId"],
                "Narrative": None,
                "StartDate": record["StartDate"],
                "EndDate": record["EndDate"],
            }

            output.append(new_record)

            api_url = 'https://api.iris.co.uk/hr/v2/attendance/absences'

            headers = {
                'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
                'Authorization': f'Bearer {cascade_token}',
                'Content-Type':'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
                    }
                                    
            response = requests.post(api_url, headers=headers, json=new_record)
            json_response = response.json()

            trackingID = json_response.get("id")
            print (f'                   {trackingID}')
                    
            if response.status_code == 201:
                json_response = response.json()
                trackingID = json_response.get("id")
            elif response.status_code == 429:
                print(f'                Failed to create absence. Rate Limit hit') 
            else:
                print("        "+f'Response Code: {response.status_code}')    
            time.sleep(0.8)  

            absence_days_from_adp(trackingID)

        if Data_export:
            file_path = os.path.join(data_store,"005 - Absences to Cascade", "010 - ADPabsences.json")
            with open(file_path, "w") as json_file:
                json.dump(output, json_file, indent=4)
        
    return (output)

def DELETE(delete_ids):

    if Data_export:
        file_path = os.path.join(data_store,"005 - Absences to Cascade", "011 - All deleted -ID.json")
        with open(file_path, "w") as json_file:
            json.dump(delete_ids, json_file, indent=4)

    for ID_to_delete in delete_ids:
        api_url = f'https://api.iris.co.uk/hr/v2/attendance/absences/{ID_to_delete}'

        headers = {
            'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
            'Authorization': f'Bearer {cascade_token}',
            'Content-Type': 'application/json;odata.metadata=minimal;odata.streaming=true; version=1',
        }
        
        response = requests.delete(api_url, headers=headers)
        
        # Check if the deletion was successful
        if response.status_code == 204:
            print(f'                Successfully deleted absence with ID: {ID_to_delete}')
        elif response.status_code == 404:
            print(f'                Failed to delete absence with ID: {ID_to_delete}. ID not recognsed in system')
        elif response.status_code == 429:
            print(f'                Failed to delete absence with ID: {ID_to_delete}. Rate Limit hit') 
        else:
            print(f'                Failed to delete absence with ID: {ID_to_delete}. Status code: {response.status_code}')
    
        time.sleep(0.6)  

#----------------------------------------------------------------------------------# Update Personal

def make_api_request(DisplayId):
    api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
    api_headers = {
        'Authorization': f'Bearer {cascade_token}',
    }

    api_params = {
        "$filter": f"DisplayId eq '{DisplayId}'",
        "$select": "DisplayId,Id,ContinuousServiceDate",
    }                
    api_response = requests.get(api_url, headers=api_headers, params=api_params)
    response_data = api_response.json()       
    cascade_id_full = response_data['value'][0]['Id']
    cont_service_raw = response_data['value'][0]['ContinuousServiceDate']
    cont_service = datetime.fromisoformat(cont_service_raw.replace("Z", "")).strftime('%Y-%m-%d')

    time.sleep(0.6)                                               #forces a wait to eliminate 429 errors
    return cascade_id_full, cont_service

def load_csv_from_bucket(name):
    bucket_name = "event_list_objects"
    file_name = f"{name}.csv"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        raise FileNotFoundError(f"The file {file_name} does not exist in bucket {bucket_name}.")

    csv_data = blob.download_as_text(encoding="utf-8-sig")
    csv_reader = csv.DictReader(StringIO(csv_data), delimiter=',')

    result = [row for row in csv_reader]

    if Data_export:
        file_path = os.path.join(data_store,"003 - Personal to Cascade",f"000 - terminations.json")
        with open(file_path, "w") as outfile:
            json.dump(result, outfile, indent=4)

    return result

def convert_adp_to_cascade_form(records,suffix,terminations,ID_library,x_months_ago=None):                         
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Converting the adp data to the cascade form (" + time_now+ ")")

    output = []       
        
    for record in records:
        workers = record["workers"]
        for worker in workers:
            active_job_position = None

            work_assignments = worker.get("workAssignments", [{}])
            for index, assignment in enumerate(work_assignments):
                if assignment.get("primaryIndicator", True):
                    active_job_position = index
                    continue
        
            gender = worker["person"]["genderCode"].get("shortName",None)
            salutation = worker["person"]["legalName"].get("preferredSalutations",[{}])[0].get("salutationCode",{}).get("shortName")
            FirstName = worker["person"]["legalName"]["givenName"]
            preffered = worker["person"]['legalName'].get("nickName",None)
            other_name =  worker["person"]["legalName"].get("middleName")
            family_name = worker["person"]["legalName"]["familyName1"]
            if c == "usa":
                display_id = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
            elif c == "can":
                display_id = worker["customFieldGroup"]["stringFields"][0].get("stringValue")
            
            WorkingStatus = worker["workerStatus"]["statusCode"]["codeValue"]
            isManager = worker["workAssignments"][active_job_position].get("managementPositionIndicator")
            start_date = worker["workAssignments"][active_job_position]["actualStartDate"]
            end_date = worker["workAssignments"][active_job_position].get("terminationDate")
            birthDate = worker["person"]["birthDate"]
            MaritalStatus = worker["person"].get("maritalStatusCode",{}).get("shortName")
            mobileOwner = worker["person"].get("communication",{}).get("mobiles",[{}])[0].get("nameCode",{}).get("codeValue")
            mobileNumber = worker["person"].get("communication",{}).get("mobiles",[{}])[0].get("formattedNumber")
            workEmailValue = worker.get("businessCommunication",{}).get("emails",[{}])[0].get("emailUri")
            address1 = worker["person"].get("legalAddress",{}).get("lineOne")
            address2 = worker["person"].get("legalAddress",{}).get("lineTwo")   
            address3 = worker["person"].get("legalAddress",{}).get("lineThree")
            address4 = worker["person"].get("legalAddress",{}).get("cityName")                
            address5 = worker["person"].get("legalAddress",{}).get("countrySubdivisionLevel1",{}).get("shortName")
            postCode = worker["person"].get("legalAddress",{}).get("postalCode")
            ADP_id = worker["workAssignments"][active_job_position]["positionID"]
            leave_reason_code = worker["workAssignments"][active_job_position].get("assignmentStatus", {}).get("reasonCode", {}).get("codeValue")
            
            leave_reason = next((termination.get("Cascade_Reason") for termination in terminations if termination.get("ADP_Code") == leave_reason_code), None)

            #Change any of the language from ADP to Iris HR        
            if WorkingStatus == "Active":
                WorkingStatus = "Current"
            if WorkingStatus == "Inactive":                     #This may be removed later - discussion needed AP/KG
                WorkingStatus = "Current"
            if WorkingStatus == "Terminated":
                converted_date = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")
                WorkingStatus = f"Left {converted_date}"

            if mobileOwner == "Personal Cell":
                mobileOwner = "Personal"
            if WorkingStatus == "Current":                      #Override for previous termination date
                end_date = None
            if mobileOwner == None:
                mobileOwner = "Personal"

            contServiceSplit = start_date  # Default value if no record is found
            Id = None

            for entry in ID_library:
                if entry["ADP_number"] == ADP_id and entry["CascadeId"] is None:
                    contService = entry["contServiceDate"]
                    contServiceSplit = contService.split("T")[0]
                    Id = None
                elif entry["CascadeId"] == display_id:
                    contService = entry["contServiceDate"]
                    contServiceSplit = contService.split("T")[0]
                    Id = entry["Cascade_full"]   
                    break  # Exit the loop once a match is found

            employment_start_date = datetime.strptime(start_date, "%Y-%m-%d")
            continuous_service_date = datetime.strptime(contServiceSplit, "%Y-%m-%d")

            if continuous_service_date > employment_start_date:
                contServiceSplit = start_date

            transformed_record = {
                "DisplayId": display_id,
                "TitleHonorific": salutation,
                "FirstName": FirstName,
                "KnownAs": preffered if preffered is not None else FirstName,
                "OtherName": other_name,
                "LastName": family_name,
                "CostCentre": None,
                "WorkingStatus": WorkingStatus,
                "IsManager": isManager,
                "NationalInsuranceNumber": ADP_id,
                "PayrollId": None,
                "TaxCode": None,
                "IncludeInPayroll": True,
                "EmploymentStartDate": start_date,
                "EmploymentLeftDate": end_date,
                "ContinuousServiceDate": contServiceSplit,
                "DateOfBirth": birthDate,
                "LastWorkingDate": end_date,
                "Gender": gender,
                "Ethnicity": None,
                "Nationality": None,
                "Religion": None,
                "LeaverReason": leave_reason,
                "MaritalStatus":MaritalStatus,
                "Phones": [
                    {
                "Ownership": mobileOwner,
                "Type" : "Mobile",
                "Value": mobileNumber,
                    }
                ],
                "Emails": [
                    {
                "Ownership": "Organization",
                "Value": workEmailValue,
                    }
                ],
                "Addresses": [
                    {
                "Ownership": "Personal",
                "Address1": address1,
                "Address2": address2,
                "Address3": address3,
                "Address4": address4,
                "Address5": address5,
                "PostCode": postCode,

                    }
                ],
                "GenderIdentity": None,
                "WindowsUsername": None,
                "Id": Id,
            }

            # Filter terminated records to only include those from the last 6 months
            if suffix == "terminated":
                if end_date:
                    try:
                        termination_date = datetime.strptime(end_date, "%Y-%m-%d")
                        if termination_date >= x_months_ago:
                            output.append(transformed_record)
                    except (ValueError, TypeError):
                        # If date parsing fails, skip this record
                        pass
            else:
                # For non-terminated records, add all records
                output.append(transformed_record)
        
        # Save individual dataset files
        if Data_export:
            file_path = os.path.join(data_store,"003 - Personal to Cascade",f"001 - ADP_to_cascade_{suffix}.json")
            with open(file_path, "w") as outfile:
                json.dump(output, outfile, indent=4)
    
    return output

def cascade_rejig_personal(cascade_responses):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the order of Cascade to allow comparison (" + time_now + ")")

    def convert_datetime_to_date(datetime_str):
        dt_object = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt_object.strftime("%Y-%m-%d")

    #Takes the raw data from Cascade and reorders into the format needed for the upload.
    cascade_reordered = [
                {
                "DisplayId": entry.get("DisplayId", ""),
                "TitleHonorific": entry.get("TitleHonorific", ""),
                "FirstName": entry.get("FirstName", ""),
                "KnownAs": entry.get("KnownAs", ""),
                "OtherName": entry.get("OtherName", ""),
                "LastName": entry.get("LastName", ""),
                "CostCentre": entry.get("CostCentre", ""),
                "WorkingStatus": "Current" if entry.get("WorkingStatus", "") == "On Holiday" else entry.get("WorkingStatus", ""),
                "IsManager": entry.get("IsManager", False),
                "NationalInsuranceNumber": entry.get("NationalInsuranceNumber", ""),
                "PayrollId": entry.get("PayrollId", ""),
                "TaxCode": entry.get("TaxCode", ""),
                "IncludeInPayroll": entry.get("IncludeInPayroll", True),
                "EmploymentStartDate": convert_datetime_to_date(entry["EmploymentStartDate"]) if entry["EmploymentStartDate"] is not None else None,
                "EmploymentLeftDate": convert_datetime_to_date(entry["EmploymentLeftDate"]) if entry["EmploymentLeftDate"] is not None else None,
                "ContinuousServiceDate": convert_datetime_to_date(entry["ContinuousServiceDate"]) if entry["ContinuousServiceDate"] is not None else None,
                "DateOfBirth": convert_datetime_to_date(entry["DateOfBirth"]) if entry["DateOfBirth"] is not None else None,
                "LastWorkingDate": convert_datetime_to_date(entry["LastWorkingDate"]) if entry["LastWorkingDate"] is not None else None,
                "Gender": entry.get("Gender", ""),
                "Ethnicity": entry.get("Ethnicity", ""),
                "Nationality": entry.get("Nationality", ""),
                "Religion": entry.get("Religion", ""),
                "LeaverReason": entry.get("LeaverReason", ""),
                "MaritalStatus": entry.get("MaritalStatus", ""),
                "Phones": entry.get("Phones", []),
                "Emails": entry.get("Emails", []),
                "Addresses": entry.get("Addresses", []),
                "GenderIdentity": entry.get("GenderIdentity", ""),
                "WindowsUsername": entry.get("WindowsUsername", ""),
                "Id": entry.get("Id", "")
                }
        for entry in cascade_responses
    ]

    if Data_export:
        file_path = os.path.join(data_store,"003 - Personal to Cascade","002 - Cascade_reordered.json")
        with open(file_path, "w") as json_file:
            json.dump(cascade_reordered, json_file, indent=4)

    return cascade_reordered

def combine_json_files(adp_to_cascade_terminated,adp_to_cascade,cascade_reordered):
    ct_combining_personal = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Generating a list of files that need updating (" + ct_combining_personal + ")")
    
    unique_entries = []
    for idx, entry in enumerate(adp_to_cascade):
        if entry not in cascade_reordered:
            adp_status = entry.get('WorkingStatus')
            cascade_entry = cascade_reordered[idx]
            cascade_status = cascade_entry.get('WorkingStatus')
            if adp_status != cascade_status and not (adp_status == 'Current' and cascade_status == 'On Holiday'):
                unique_entries.append((entry, idx))
    
    update_personal = [entry for entry, _ in unique_entries]
    idx_list = [idx for _, idx in unique_entries]
    idx_tuple = tuple(idx_list)

    new_starters = [entry for entry in adp_to_cascade if entry.get('DisplayId') in [None,""]]
    print("             New Staff: "+str(len(new_starters)))

    update_personal = [entry for entry in update_personal if entry.get('DisplayId') not in [None,""]]
    print("             Updating Staff: "+str(len(update_personal)))

    # New Section: Filter out terminated staff that are still present in cascade_reordered
    #cascade_ids = {entry.get("DisplayId") for entry in cascade_reordered}
    unterminated_staff = [entry for entry in adp_to_cascade_terminated if entry in cascade_reordered]
    print("             Terminated Staff not in Cascade: " + str(len(unterminated_staff)))

    processed_unterminated_records = []

    for record in unterminated_staff:
        # Extract DisplayId
        display_id = record.get("DisplayId")
        
        if display_id:
            # Call the API function
            new_id,cont_service = make_api_request(display_id)
            
            # Create a copy of the record and update the Id
            updated_record = record.copy()
            updated_record["Id"] = new_id
            updated_record["ContinuousServiceDate"] = cont_service
            
            processed_unterminated_records.append(updated_record)
            
            print(f"Updated record for DisplayId {display_id}: new Id = {new_id}")
        else:
            print(f"Warning: No DisplayId found in record")
            processed_unterminated_records.append(record)

    for entry in new_starters:
        if 'Id' in entry:
            del entry['Id']
    
    if Data_export:
        file_path_out = os.path.join(data_store,"003 - Personal to Cascade","003a - Non Matching records.json")
        with open(file_path_out, 'w') as output_file:
            json.dump(unique_entries, output_file, indent=2)
    
        file_path_out = os.path.join(data_store,"003 - Personal to Cascade", "003b - Updated Records.json")
        with open(file_path_out, 'w') as output_file:
            json.dump(update_personal, output_file, indent=2)

        file_path_out = os.path.join(data_store,"003 - Personal to Cascade", "003c - New Starters.json")
        with open(file_path_out, 'w') as output_file:
            json.dump(new_starters, output_file, indent=2)
    
        file_path_out = os.path.join(data_store, "003 - Personal to Cascade", "003d - Terminated Staff.json")
        with open(file_path_out, 'w') as output_file:
            json.dump(processed_unterminated_records, output_file, indent=2)

    return update_personal, new_starters, processed_unterminated_records

def PUT_cascade_workers_personal(list_of_staff):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Updating Staff changes (" + time_now + ")")                         
    
    for entry in list_of_staff:
        employee_id = entry.get("Id")
        display_id = entry.get("DisplayId")
        FirstName = entry.get("FirstName")
        LastName = entry.get("LastName")

        # Check if employee_id is null
        if employee_id is None:
            print("        " + f'No cascade ID for employee {display_id} ({FirstName} {LastName}). Skipping this record.')
            continue

        transformed_record = entry
                
        api_base = 'https://api.iris.co.uk/hr/v2/employees/'
        cascade_identifier = employee_id
    
        api_url = api_base + cascade_identifier         

        headers = {
            'Authorization': f'Bearer {cascade_token}',
            'Content-Type':'text/json; version=2',
            'Content-Length': '22',
        }
        
        
        response = requests.put(api_url, headers=headers, json=transformed_record)
        
        if response.status_code == 204:
            print("             " + f'Personal information transfer for {FirstName} {LastName} ({display_id}) complete. {response.status_code}')
        else:
            print("             " + f'Data Transfer for {FirstName} {LastName} - {display_id} has failed. Response Code: {response.status_code}')           
        time.sleep(0.75) 
    
        #input("Enter to continue")

def POST_new_starters(new_starters): 
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Adding new staff (" + time_now + ")")                              
    for entry in new_starters:
        FirstName = entry.get("FirstName")
        LastName = entry.get("LastName")
        entry["DisplayId"] = None

        transformed_record = entry
        api_url = 'https://api.iris.co.uk/hr/v2/employees'
        
        if Data_export:
            file_path = os.path.join(data_store,"003 - Personal to Cascade","005 - New Start.json")
            with open(file_path, "w") as outfile:
                json.dump(transformed_record, outfile, indent=4)


        headers = {
            'Authorization': f'Bearer {cascade_token}',
            'Content-Type':'text/json; version=2',
            'Content-Length': '22',
        }
        
        response = requests.post(api_url, headers=headers, json=transformed_record)
        
        if response.status_code == 201:
            print("             " + f'New Starter Added ({FirstName} {LastName})')
        else:
            print("             "+f'Data Transfer for New Starter ({FirstName} {LastName}) has failed. Response Code: {response.status_code}')           
        time.sleep(0.75)  
            
#----------------------------------------------------------------------------------# Update Jobs

def cascade_current_workers():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Retrieving Job Data from Cascade HR (" + time_now + ")")

    api_url = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'
    cascade_job_responses = []

    def make_api_request(skip_param):
        api_headers = {
            'Authorization': f'Bearer {cascade_token}',
        }
        api_params = {
            "$filter": "EndDate eq null",
            "$top": 250,
            "$skip": skip_param
        }
    
        api_response = requests.get(api_url, headers=api_headers, params=api_params)
        time.sleep(0.6)

        if api_response.status_code == 200:
            json_data = api_response.json()
            cascade_job_responses.append(json_data)

            if not json_data:
                return True
        
        else:
            print(f"            Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    total_records = 0
    skip_param = 0

    while total_records <= 4000:
        if make_api_request(skip_param):
            break
        skip_param += 250
        total_records += 250
    
    combined_value=[]
    for item in cascade_job_responses:
        combined_value.extend(item["value"])

    combined_data = [{
        "value": combined_value,
    }]

    if Data_export:
        file_path = os.path.join(data_store,"004 - Jobs to Cascade","001 - Cascade Jobs.json")
        with open(file_path, "w") as outfile:
            json.dump(combined_data, outfile, indent=4)
    
    return combined_data

def cascade_rejig_jobs(cascade_current_jobs):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the order of Cascade to allow comparison (" + time_now + ")")

    combined_data = [entry for response in cascade_current_jobs for entry in response.get('value', [])]

    def convert_datetime_to_date(datetime_str):
        dt_object = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt_object.strftime("%Y-%m-%d")

    cascade_reordered = [
                {
                "JobTitle": entry.get("JobTitle"),
                "Classification": entry.get("Classification"),
                "StartDate": convert_datetime_to_date(entry["StartDate"]) if entry["StartDate"] is not None else None,
                "EndDate": convert_datetime_to_date(entry["EndDate"]) if entry["EndDate"] is not None else None,
                "WorkingCalendar": "40hrs Monday to friday",                                        #entry.get("WorkingCalendar"),
                "LineManagerId": entry.get("LineManagerId"),
                "HierarchyNodeId": entry.get("HierarchyNodeId"),
                "Active": entry.get("Active"),
                "Salary": float(Decimal(entry.get("Salary", 0.0) or 0.0).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)),
                "EmployeeId": entry.get("EmployeeId"),
                "Contract": "Permenent",                                        #Needs to be changed once the updating is sorted
                "PayFrequency": "Biweekly" if entry.get("PayFrequency") == "Fortnightly" else entry.get("PayFrequency"),
                "PayBasis": "Yearly" if entry.get("PayBasis") == "ANNUAL" else entry.get("PayBasis"),
                "FullTimeEquivalent": 1,
                "ChangeReason": entry.get("ChangeReason"),
                "NextIncrementDate": entry.get("NextIncrementDate"),
                "TimesheetLocation": entry.get("TimesheetLocation"),
                "TimesheetLunchDuration": None if entry.get("TimesheetLunchDuration") is not None else entry.get("TimesheetLunchDuration"),
                "ExpenseSubmissionFrequency": entry.get("ExpenseSubmissionFrequency") if entry.get("ExpenseSubmissionFrequency") !="" else None,
                "CostCentre": entry.get("CostCentre") if entry.get("CostCentre") != "" else None,
                "JobFamily": entry.get("JobFamily"),
                "ApprenticeUnder25": entry.get("ApprenticeUnder25"),
                "ApprenticeshipEndDate": convert_datetime_to_date(entry["ApprenticeshipEndDate"]) if entry["ApprenticeshipEndDate"] is not None else None,
                "ContractEndDate": convert_datetime_to_date(entry["ContractEndDate"]) if entry["ContractEndDate"] is not None else None,
                "NormalHours": entry.get("NormalHours"),
                "RealTimeInformationIrregularFrequency": entry.get("RealTimeInformationIrregularFrequency") if entry.get("RealTimeInformationIrregularFrequency") !="" else None,
                "NoticePeriod": entry.get("NoticePeriod"),
                "Id": entry.get("Id"),
                }
        for entry in combined_data
    ]

    for record in cascade_reordered:
        record["StartDate"] = datetime.strptime(record["StartDate"], "%Y-%m-%d")
    
    most_recent_records = {}

    for record in cascade_reordered:
        employee_id = record["EmployeeId"]
        if employee_id not in most_recent_records or record["StartDate"] > most_recent_records[employee_id]["StartDate"]:
            most_recent_records[employee_id] = record
    
    for record in most_recent_records.values():
        record["StartDate"] = record["StartDate"].strftime("%Y-%m-%d")

    filtered_records = list(most_recent_records.values())



    if Data_export:
        file_path = os.path.join(data_store,"004 - Jobs to Cascade","002 - Cascade_reordered.json")
        with open(file_path, "w") as json_file:
            json.dump(filtered_records, json_file, indent=4)

    return filtered_records

def adp_rejig(cascade_current,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the Jobs info to upload to Cascade (" + time_now + ")")


    transformed_records = []        
    records_contained = []

    for record in adp_responses:

        workers = record["workers"]
        for worker in workers:
            work_assignments = worker.get("workAssignments", [{}])

            active_job_position = None

            for index, assignment in enumerate(work_assignments):
                if assignment.get("primaryIndicator", True):
                    active_job_position = index
                    continue
            
            jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
            hireDate = worker["workAssignments"][active_job_position].get("actualStartDate")
            paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
            paybasis_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("nameCode", {}).get("shortName", None)
            pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
            pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
            additional_pay_hourly = (
                worker.get("workAssignments", [{}])[0]
                .get("additionalRemunerations", [{}])[0]
                .get("rate", {})
                .get("amountValue")
                )
            effective_date_wage = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
            effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")

            pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)
            home_units = worker["workAssignments"][active_job_position].get("homeOrganizationalUnits", [{}])
            if len(home_units) == 2:
                adp_hierarchy = home_units[1].get("nameCode", {}).get("codeValue", "")
            else:
                adp_hierarchy = home_units[0].get("nameCode", {}).get("codeValue", "")
            if c == "usa":
                contract = worker["workAssignments"][active_job_position]["workerGroups"][0]["groupCode"].get("codeValue","")
            else:
                contract = worker["workAssignments"][active_job_position]["workerTypeCode"].get("codeValue","")                
            ADP_id = worker["workAssignments"][active_job_position]["positionID"]
            LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)

            for record in ID_library:
                if record["ADP_number"]==ADP_id:
                    employee_id = record["Cascade_full"]
                    hierarchy_id = record["Hierarchy"]

            for record in ID_library:
                if record["AOID"]==LM_AOID:
                    line_manager = record["Cascade_full"]
            
            file_path = os.path.join(current_folder,"Hierarchy.xlsx")
            df = pd.read_excel(file_path, sheet_name='JJ')
            reports_to_JJ = df['ID'].tolist()
            if employee_id in reports_to_JJ:
                line_manager = "b3775d20-8d33-4ca9-aaad-5e2346bb17e9"
            else:
                line_manager = line_manager

            if paybasis_hourly is not None:
                paybasis = "Hourly"
            elif paybasis_annual is not None:
                paybasis = "Yearly"
            else:
                continue                

            if pay_hourly is not None:
                salary = float(pay_hourly)
            else:
                salary = float(pay_annual)
            
            salary_rounded = round(salary,2)

            if contract == "Full Time":
                contract = "Permenent"
            elif contract == "FT":
                contract = "Permenent"
            elif contract == "Regular Full-Time":
                contract = "Permenent"
            else:
                contract = "Temporary"

            for record in cascade_current:
                if record.get("EmployeeId") == employee_id:
                    Id = record.get("Id")
                    jobTitle = record.get("JobTitle")
                    JobFamily = record.get("JobFamily")
                    cascadeStart = record.get("StartDate")
                    notice = record.get("NoticePeriod")
                    classification = record.get("Classification")

                    wage = datetime.strptime(effective_date_wage, "%Y-%m-%d")
                    cascade = datetime.strptime(cascadeStart, "%Y-%m-%d")
                    other = datetime.strptime(effective_date_other, "%Y-%m-%d")

                    startDate = max(wage, cascade, other)
                    startDate = startDate.strftime("%Y-%m-%d")

                    effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")

                    if str(record.get("Salary")) != str(salary):
                        ChangeReason = "Change of Salary"
                    elif str(record.get("HierarchyNodeId")) != str(hierarchy_id):
                        ChangeReason = "Change of Position"
                    elif str(record.get("LineManagerId")) != str(line_manager):
                        ChangeReason = "Change of Manager"
                    elif str(record.get("StartDate")) != str(startDate):
                        ChangeReason = "Minor Change/Correction"
                    else:
                        ChangeReason = record.get("ChangeReason")

                    transformed_record = {
                        "JobTitle": jobTitle,
                        "Classification": classification,
                        "StartDate": startDate,                  
                        "EndDate": None,
                        "WorkingCalendar": "40hrs Monday to friday", 
                        "LineManagerId": line_manager,
                        "HierarchyNodeId": hierarchy_id,
                        "Active": True,
                        "Salary": salary_rounded,
                        "EmployeeId": employee_id,
                        "Contract": contract,
                        "PayFrequency": pay_frequency,
                        "PayBasis": paybasis,
                        "FullTimeEquivalent": 1,                                                    #This is likely to change once working patterns in the US are described.
                        "ChangeReason": ChangeReason,        
                        "NextIncrementDate": None,
                        "TimesheetLocation": None,
                        "TimesheetLunchDuration": None,
                        "ExpenseSubmissionFrequency": None,
                        "CostCentre": None,
                        "JobFamily": JobFamily,
                        "ApprenticeUnder25": None,
                        "ApprenticeshipEndDate": None,
                        "ContractEndDate": None,
                        "NormalHours": 40,
                        "RealTimeInformationIrregularFrequency": None,
                        "NoticePeriod": notice,
                        "Id": Id                            
                        }
                
                    records_to_add = {
                        "EmployeeId": employee_id,
                    }


                    transformed_records.append(transformed_record)
                    records_contained.append(records_to_add)
                
    if Data_export:
        file_path = os.path.join(data_store,"004 - Jobs to Cascade","003a - ADP_reordered (Staff with roles).json")
        with open(file_path, "w") as json_file:
            json.dump(transformed_records, json_file, indent=4)

        file_path = os.path.join(data_store,"004 - Jobs to Cascade","003b - ADP_reordered (Staff with roles - Id).json")
        with open(file_path, "w") as json_file:
            json.dump(records_contained, json_file, indent=4)

    employee_ids = {record["EmployeeId"] for record in records_contained}

    new_start_jobs = [
        {"AOID":entry["AOID"]}
        for entry in ID_library
        if entry["Cascade_full"] not in employee_ids
    ]

    if Data_export:
        file_path = os.path.join(data_store,"004 - Jobs to Cascade","003c - ADP_reordered (New Starters).json")
        with open(file_path, "w") as json_file:
            json.dump(new_start_jobs, json_file, indent=4)

                
    return transformed_records,new_start_jobs   

def adp_rejig_new_starters(new_starters,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the New Staff Jobs info to upload to Cascade (" + time_now + ")")
    new_start = []
    transformed_records = []        

    new_starter_values = [entry["AOID"] for entry in new_starters]

    for response in adp_responses:
        workers = response.get("workers", [])
        filtered_workers = [
            worker for worker in workers if worker.get("associateOID") in new_starter_values
        ]

        if filtered_workers:
            response["workers"] = filtered_workers
            new_start.append(response)

    for record in new_start:

        workers = record["workers"]
        for worker in workers:
            work_assignments = worker.get("workAssignments", [{}])

            active_job_position = None

            for index, assignment in enumerate(work_assignments):
                if assignment.get("primaryIndicator", True):
                    active_job_position = index
                    continue

            #classification = worker["workAssignments"][active_job_position].get("occupationalClassifications", [{}])[0].get("classificationCode", {}).get("longName") or worker["workAssignments"][active_job_position].get("occupationalClassifications", [{}])[0].get("classificationCode", {}).get("shortName")
            jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
            hireDate = worker["workAssignments"][active_job_position].get("actualStartDate")
            paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
            paybasis_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("nameCode", {}).get("shortName", None)
            pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
            pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
            additional_pay_hourly = (
                worker.get("workAssignments", [{}])[0]
                .get("additionalRemunerations", [{}])[0]
                .get("rate", {})
                .get("amountValue")
                )            
            effective_date = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
            pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)
            home_units = worker["workAssignments"][active_job_position].get("homeOrganizationalUnits", [{}])
            if len(home_units) == 2:
                adp_hierarchy = home_units[1].get("nameCode", {}).get("codeValue", "")
            else:
                adp_hierarchy = home_units[0].get("nameCode", {}).get("codeValue", "")
            if c == "usa":
                contract = worker["workAssignments"][active_job_position]["workerGroups"][0]["groupCode"].get("codeValue","")
            else:
                contract = worker["workAssignments"][active_job_position]["workerTypeCode"].get("codeValue","")
            ADP_id = worker["workAssignments"][active_job_position]["positionID"]
            LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)
        
            for record in ID_library:
                if record["ADP_number"]==ADP_id:
                    employee_id = record["Cascade_full"]
                    hierarchy_id = record["Hierarchy"]              

            for record in ID_library:
                if record["AOID"]==LM_AOID:
                    line_manager = record["Cascade_full"]

            file_path = os.path.join(current_folder,"Hierarchy.xlsx")
            df = pd.read_excel(file_path, sheet_name='JJ')
            reports_to_JJ = df['ID'].tolist()
            if employee_id in reports_to_JJ:
                line_manager = "b3775d20-8d33-4ca9-aaad-5e2346bb17e9"
            else:
                line_manager = line_manager

            if paybasis_hourly is not None:
                paybasis = "Hourly"
            else:
                paybasis = "Yearly"

            if pay_hourly is not None:
                salary = pay_hourly
            else:
                salary = pay_annual

            
            if contract == "Regular Full-Time":
                contract = "Permenant"
            elif contract =="FT":
                contract = "Permenant"
            else:
                contract = "Temporary"
    
            transformed_record = {
                "JobTitle": jobTitle,
                "Classification": None,
                "StartDate": hireDate,                  
                "EndDate": None,
                "WorkingCalendar": "40hrs Monday to friday", 
                "LineManagerId": line_manager,
                "HierarchyNodeId": hierarchy_id,
                "Active": True,
                "Salary": salary,
                "EmployeeId": employee_id,
                "Contract": contract,
                "PayFrequency": pay_frequency,
                "PayBasis": paybasis,
                "FullTimeEquivalent": 1,                                                    #This is likely to change once working patterns in the US are described.
                "ChangeReason": "New Starter",        
                "NextIncrementDate": None,
                "TimesheetLocation": None,
                "TimesheetLunchDuration": None,
                "ExpenseSubmissionFrequency": None,
                "CostCentre": None,
                "JobFamily": None,
                "ApprenticeUnder25": None,
                "ApprenticeshipEndDate": None,
                "ContractEndDate": None,
                "NormalHours": 40,
                "RealTimeInformationIrregularFrequency": None,
                "NoticePeriod": None,
            }

            transformed_records.append(transformed_record)

    if Data_export:
        file_path_out = os.path.join(data_store, "004 - Jobs to Cascade", "004d - New Starter Jobs.json")
        with open(file_path_out, 'w') as output_file:
            json.dump(transformed_records, output_file, indent=2)

    return transformed_records
                        
def classify_adp_files(new_start_jobs,adp_current,cascade_current):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Classify current staff jobs as new line or updated record (" + time_now + ")")
    not_to_be_updated = []
    PUT_jobs = []
    POST_jobs = []

    for adp_record in adp_current:
        matched = False
        for cascade_record in cascade_current:
            if adp_record == cascade_record:                                                                                                    # Compares and finds fully matching records                      
                not_to_be_updated.append(adp_record)
                matched = True
                break
            elif (adp_record["EmployeeId"] == cascade_record["EmployeeId"] and                                                                  # Same EmployeeId and StartDate but a different field - Needs updating   
                adp_record["StartDate"] == cascade_record["StartDate"]):
                PUT_jobs.append(adp_record)
                matched = True
                break
            elif adp_record["EmployeeId"] == cascade_record["EmployeeId"] and adp_record["StartDate"] != cascade_record["StartDate"]:
                POST_jobs.append(adp_record)                                                                                                    # Different StartDate - Needs a new line
                matched = True
                break

        # If no match is found in cascade_current, determine which list to add it to
        if not matched:
            if any(adp_record["EmployeeId"] == rec["EmployeeId"] for rec in cascade_current):
                if any(adp_record["StartDate"] == rec["StartDate"] for rec in cascade_current):
                    PUT_jobs.append(adp_record)
                else:
                    POST_jobs.append(adp_record)
    
    print (f"           {len(not_to_be_updated)} records do not need to be updated.")
    print (f"           {len(PUT_jobs)} records DO need to be updated.")
    print (f"           {len(POST_jobs)} records have changed and need a new line.")
    print (f"           {len(new_start_jobs)} records do not yet have a job on Cascade")
    print ()

    if Data_export:
        no_update_jobs_path = os.path.join(data_store, "004 - Jobs to Cascade", "005a - No update needed.json")
        update_jobs_path = os.path.join(data_store, "004 - Jobs to Cascade", "005b - update current jobs.json")
        add_jobs_path = os.path.join(data_store, "004 - Jobs to Cascade", "005c - add job line.json")
        
        with open(no_update_jobs_path, 'w') as update_file:
            json.dump(not_to_be_updated, update_file, indent=2)

        with open(update_jobs_path, 'w') as update_file:
            json.dump(PUT_jobs, update_file, indent=2)
        
        with open(add_jobs_path, 'w') as add_file:
            json.dump(POST_jobs, add_file, indent=2)

    return PUT_jobs, POST_jobs

def PUT_update_job_change(PUT_jobs):
    
    print ("            Updating records that are already present")
    for record in PUT_jobs:
        update_record = {
        "JobTitle": record["JobTitle"],
        "Classification": record["Classification"],
        "StartDate": record["StartDate"],
        "EndDate": record["EndDate"],
        "WorkingCalendar": record["WorkingCalendar"],
        "LineManagerId": record["LineManagerId"],
        "HierarchyNodeId": record["HierarchyNodeId"],
        "Active": record["Active"],
        "Salary": record["Salary"],
        "EmployeeId": record["EmployeeId"],
        "Contract": record["Contract"],
        "PayFrequency": record["PayFrequency"],
        "PayBasis": record["PayBasis"],
        "FullTimeEquivalent": record["FullTimeEquivalent"],
        "ChangeReason": record["ChangeReason"],
        "NextIncrementDate": record["NextIncrementDate"],
        "TimesheetLocation": record["TimesheetLocation"],
        "TimesheetLunchDuration": record["TimesheetLunchDuration"],
        "ExpenseSubmissionFrequency": record["ExpenseSubmissionFrequency"],
        "CostCentre": record["CostCentre"],
        "JobFamily": record["JobFamily"],
        "ApprenticeUnder25": record["ApprenticeUnder25"],
        "ApprenticeshipEndDate": record["ApprenticeshipEndDate"],
        "ContractEndDate": record["ContractEndDate"],
        "NormalHours": record["NormalHours"],
        "RealTimeInformationIrregularFrequency": record["RealTimeInformationIrregularFrequency"],
        "NoticePeriod": record["NoticePeriod"],
        "Id": record["Id"]
        }
        Id = record["Id"]
        employeeId = record["EmployeeId"]

        api_base = 'https://api.iris.co.uk/hr/v2/jobs/'
        api_url = api_base + Id      

        headers = {
            'Authorization': f'Bearer {cascade_token}',
            'Content-Type':'text/json; version=2',
            'Content-Length': '22',
        }
        
        response = requests.put(api_url, headers=headers, json=update_record)
        
        if response.status_code == 204:
            print("        " + f'Current Job updated for {employeeId} complete')
        else:
            print("        "+f'Data Transfer for {employeeId} has failed. Response Code: {response.status_code}')           
        time.sleep(0.76)

def POST_create_jobs(POST_jobs, new_start_jobs):
                
        print ("            Adding new job lines")
        combined_list = POST_jobs + new_start_jobs

        for record in combined_list:
            update_record = {
            "JobTitle": record["JobTitle"],
            "Classification": record["Classification"],
            "StartDate": record["StartDate"],
            "EndDate": record["EndDate"],
            "WorkingCalendar": record["WorkingCalendar"],
            "LineManagerId": record["LineManagerId"],
            "HierarchyNodeId": record["HierarchyNodeId"],
            "Active": record["Active"],
            "Salary": record["Salary"],
            "EmployeeId": record["EmployeeId"],
            "Contract": record["Contract"],
            "PayFrequency": record["PayFrequency"],
            "PayBasis": record["PayBasis"],
            "FullTimeEquivalent": record["FullTimeEquivalent"],
            "ChangeReason": record["ChangeReason"],
            "NextIncrementDate": record["NextIncrementDate"],
            "TimesheetLocation": record["TimesheetLocation"],
            "TimesheetLunchDuration": record["TimesheetLunchDuration"],
            "ExpenseSubmissionFrequency": record["ExpenseSubmissionFrequency"],
            "CostCentre": record["CostCentre"],
            "JobFamily": record["JobFamily"],
            "ApprenticeUnder25": record["ApprenticeUnder25"],
            "ApprenticeshipEndDate": record["ApprenticeshipEndDate"],
            "ContractEndDate": record["ContractEndDate"],
            "NormalHours": record["NormalHours"],
            "RealTimeInformationIrregularFrequency": record["RealTimeInformationIrregularFrequency"],
            "NoticePeriod": record["NoticePeriod"]
            }
            employeeId = record["EmployeeId"]

            api_url = 'https://api.iris.co.uk/hr/v2/jobs'

            headers = {
                'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=2',
                'Authorization': f'Bearer {cascade_token}',
                'Content-Type':'text/json; version=2',
#                'Content-Length': '22',
            }
            
            response = requests.post(api_url, headers=headers, json=update_record)
            
            if response.status_code == 201:
                print("        " + f'New Job line added for {employeeId} complete')
            else:
                print("        "+f'Data Transfer for {employeeId} has failed. Response Code: {response.status_code}')           
            time.sleep(0.76)

#----------------------------------------------------------------------------------# 
if __name__ == "__main__":

    delete_folders()                                #clears out at the start of every run. Can be recreated if needed
    time.sleep(1)
    global country,creds,project_Id,storage_client

    extended_update,Data_export = debug_check(debug)

    run_type = find_run_type()
    #run_type = 1                                    #Comment this out in the production version

    creds, project_Id = google_auth()

    x_months_ago = datetime.now() - timedelta(days=180)
    storage_client = storage.Client(credentials=creds,project=project_Id)

    def country_choice(c,run_type,debug):
        print ("---------------------------------------------------------------------------------------------------------------")
        print (f"Synchronizing country: {c}")                                           #c represents country. Either USA or CAN


        global access_token, cascade_token, certfile, keyfile, strings_to_exclude, extended_update
        global Data_export, data_store,country_hierarchy_USA, country_hierarchy_CAN
        
        data_store = data_store_location(c)

        client_id, client_secret, strings_to_exclude, country_hierarchy_USA, country_hierarchy_CAN, cascade_API_id, keyfile, certfile = load_keys(c)

        temp_certfile, temp_keyfile = load_ssl(certfile, keyfile)
        
        keyfile, certfile, access_token, cascade_token  = security(client_id, client_secret, cascade_API_id, temp_certfile, temp_keyfile)
        
        #----------     Global Data Calls     ----------#
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Making global calls (" + time_now + ")")
        adp_responses, adp_terminations         = GET_workers_adp()
        cascade_responses                       = GET_workers_cascade()
        hierarchy_nodes                         = GET_hierarchy_nodes(c)
        ID_library                              = ID_generator(c,adp_responses,cascade_responses,hierarchy_nodes)

        #----------     Push New Cascade Id's back to Cascade     ----------#
        if run_type == 1:
            
            time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print ("    Pushing Cascade Id's back to ADP (" + time_now + ")")

            CascadeId_to_upload             = whats_in_ADP(adp_responses, ID_library, c)
            upload_cascade_Ids_to_ADP(CascadeId_to_upload,c)

        #----------     Delete removed Absences     ----------#

        elif run_type in [2, 5]:
            all_absences = []

            if c == "usa":
                absences = load_from_bucket("Absences")
                cancellations = load_from_bucket("Cancellations")
                ID_list = [record for record in ID_library if record["AOID"] in absences or record["AOID"] in cancellations]
            
            else:
                ID_list = ID_library

            ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
            absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

            absence_reasons = create_absences_reasons()

            for record in ID_list:
                CascadeId = record["CascadeId"]
                print(f"Updating absences for {CascadeId}")
                Cascade_full, AOID = get_cascade_id(CascadeId,ID_library)            
                
                try:
                    adp_response = get_absences_adp(AOID)                               #Downloads the absences in the last 90 days for a given staff member
                    adp_current = convert_ADP_absences_to_cascade_format(adp_response,absence_reasons,Cascade_full,AOID,ninety_days_ago)              #Converts ADP absences into Cascade format

                    if len(adp_current) == 0:
                        print(f"        No booked absences for {CascadeId}")
                        continue  # If there are no absences, skip to the next record
                    else:
                        cascade_current, current_absence_id_cascade = cascade_absences(Cascade_full,absences_from)  # Pulls list of current absences
                        new_records, Update_transformed, delete_ids, update_ids = combine_json_files_for_POST(current_absence_id_cascade,adp_current,cascade_current)  # Compares adp and cascade and removes any that are already in cascade

                    DELETE(delete_ids)  # Deletes cancelled absences'''
                    if run_type ==5:
                        POST(new_records,adp_response,Cascade_full)  # Creates new absences

                except json.JSONDecodeError as e:
                    if str(e) == "Expecting value: line 1 column 1 (char 0)":
                        print("         No absences booked within the last 90 days")
                    else:
                        print(f"JSON decoding error: {e}")
                except Exception as e:
                    line_number = sys.exc_info()[-1].tb_lineno
                    error_message = f"      Error processing CascadeId {CascadeId} on line {line_number}: {e}"
                    print(error_message)
                    continue
       
       #----------     Update Personal Data     ----------#
    
        elif run_type == 3:
            time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print ("    Updating personal details on Cascade (" + time_now + ")")

            if c == "can":
                terminations = load_csv_from_bucket("CAN_termination_mapping")
            elif c == "usa":
                terminations = load_csv_from_bucket("USA_termination_mapping")

            adp_to_cascade                                              = convert_adp_to_cascade_form(adp_responses,"all",terminations,ID_library)
            adp_to_cascade_terminated                                   = convert_adp_to_cascade_form(adp_terminations,"terminated",terminations,ID_library,x_months_ago)

            cascade_reordered                                           = cascade_rejig_personal(cascade_responses)
            records_to_upload, new_starters, unterminated_staff         = combine_json_files(adp_to_cascade_terminated,adp_to_cascade,cascade_reordered)
            PUT_cascade_workers_personal(records_to_upload)
            PUT_cascade_workers_personal(unterminated_staff)
            POST_new_starters(new_starters)

        #----------     Update Job Data     ----------#
        
        elif run_type == 4:
            
            time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print ("    Updating Job details on Cascade (" + time_now + ")")
            
            cascade_current_jobs                = cascade_current_workers()
            cascade_current                     = cascade_rejig_jobs(cascade_current_jobs)
            adp_current,new_starters_jobs       = adp_rejig(cascade_current,adp_responses,ID_library)
            new_start_jobs                      = adp_rejig_new_starters(new_starters_jobs,adp_responses,ID_library)
            PUT_jobs, POST_jobs                 = classify_adp_files(new_start_jobs,adp_current,cascade_current)
            PUT_update_job_change(PUT_jobs)
            POST_create_jobs(POST_jobs, new_start_jobs)

        else:
            print("Run Type not defined correctly. Set Flag correctly")       

        ID_library = []


    countries = ["usa","can"]
    #countries = [ "can"]           #Use to test Canada independently)

    for c in countries:
        country_choice (c,run_type,debug)

    ct_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ()
    print ("Finished - Putting up my feet (" + ct_fin + ")")
    print ("---------------------------------------------------------------------------------------------------------------")


