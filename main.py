# Standard Library - Core
import os
import io
import sys
import json
import math
import tempfile
from pathlib import Path

# Standard Library - Time/Date
import time
from datetime import datetime, timedelta,time as dt_time
from zoneinfo import ZoneInfo

# Standard Library - Data Processing
import csv
from io import StringIO
from decimal import Decimal, ROUND_HALF_UP
import shutil

# Third-party - Data Processing
import pandas as pd
import numpy as np
import requests

# Google Cloud Platform
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from google.cloud import secretmanager
from google.cloud import storage

debug = False
testing = False

current_folder = Path(__file__).resolve().parent

adp_workers = 'https://api.adp.com/hr/v2/workers'
cascade_workers = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
cascade_workers_base = 'https://api.iris.co.uk/hr/v2/employees'
cascade_jobs_url = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'
cascade_absences_url = 'https://api.iris.co.uk/hr/v2/attendance/absences'        
cascade_absencedays = 'https://api.iris.co.uk/hr/v2/attendance/absencedays'


# Set up

def find_run_type():
    
    now_uk = datetime.now(ZoneInfo("Europe/London"))
    is_bst = bool(now_uk.dst())
    #is_bst = False

    print("Current UK time:", now_uk)
    print("BST active?", is_bst)
    
    # Get the current UK time (not system time)
    current_time = now_uk.time()
    #current_time = dt_time(3,30,0)     #Testing the triggering from gcs

    
    # Adjust time ranges based on BST (add 1 hour during summer)
    hour_offset = 1 if is_bst else 0
    
    # Base times (these are the winter times)
    base_time_ranges = [
        (dt_time(22, 46), dt_time(23, 15), 1),       # Push New Cascade Id's back to Cascade (23:00)
        (dt_time(23, 15), dt_time(23, 45), 2),        # Delete removed Absences (00:00)
        (dt_time(0, 46), dt_time(1, 15), 3),        # Updates staff personal and adds new staff (01:00)
        (dt_time(2, 45), dt_time(3, 15), 1),        # Push New Cascade Id's back to Cascade (Pushes ID for new Staff) (03:00)
        (dt_time(3, 16), dt_time(3, 45), 4),        # Updates job details (03:30)
        (dt_time(3, 46), dt_time(4, 15), 5),        # Adds in new and changed Absences (04:00)
    ]
    
    # Adjust time ranges for BST
    time_ranges = []
    for start_time, end_time, run_type in base_time_ranges:
        # Add hour offset for BST
        new_start = dt_time((start_time.hour + hour_offset) % 24, start_time.minute)
        new_end = dt_time((end_time.hour + hour_offset) % 24, end_time.minute)
        time_ranges.append((new_start, new_end, run_type))
        
    # Find matching time range
    for start_time, end_time, run_type in time_ranges:
        if start_time <= current_time < end_time:
            return run_type
    
    # Default run type if no time range matches
    return 1

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
    
    time.sleep(1)

def export_data(folder, filename, variable):
    file_path = Path(data_store) / folder / filename
    with open(file_path, "w") as outfile:
        json.dump(variable, outfile, indent=4)

def import_data(folder,filename):
    file_path = Path(data_store) / folder / filename
    with open(file_path, "r") as file:
        return json.load(file)

def load(folder,filename,variable_name):
    file_path = Path(data_store) / folder / filename
    with open(file_path,"r") as file:
        globals()[variable_name] = json.load(file)

def google_auth():
    try:
        # 1. Try Application Default Credentials (Cloud Run)
        credentials, project_id = default()
        print("✅ Authenticated with ADC")
        return credentials, project_id

    except DefaultCredentialsError:
        print("⚠️ ADC not available, trying GOOGLE_CLOUD_SECRET env var...")

        # 2. Codespaces (secret stored in env var)
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if secret_json:
            service_account_info = json.loads(secret_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get('project_id')
            print("✅ Authenticated with service account from env var")
            return credentials, project_id

        # 3. Local dev (service account file path)
        file_path = os.getenv("GCP")
        if file_path and os.path.exists(file_path):
            credentials = service_account.Credentials.from_service_account_file(file_path)
            with open(file_path) as f:
                project_id = json.load(f).get("project_id")
            print("✅ Authenticated with service account from file")
            return credentials, project_id

        raise Exception("❌ No valid authentication method found")

def debug_check(debug):
    if debug:
        if testing is False:
            create_folders(current_folder)     
        extended_update = False                                          
        Data_export = True
    
    else:
        extended_update = True                                                            
        Data_export = False
                    
    return extended_update,Data_export

def data_store_location(country):
    folder_name = f"Data - {country.upper()}"
    return os.path.join(current_folder, "Data Store", folder_name)

def get_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_Id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def load_keys(country):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"    Gathering Security Information ({now_str})")
    print(f"        Loading Security Keys ({now_str})")

    # Secrets to load
    secret_ids = {
        "client_id": f"ADP-{country}-client-id",
        "client_secret": f"ADP-{country}-client-secret",
        "country_hierarchy_USA": "country_Hierarchy_USA",
        "country_hierarchy_CAN": "country_Hierarchy_CAN",
        "strings_to_exclude": "strings_to_exclude",
        "cascade_API_id": "cascade_API_id",
        "keyfile": f"{country}_cert_key",
        "certfile": f"{country}_cert_pem",
    }

    secrets = {k: get_secret(v) for k, v in secret_ids.items()}

    return (
        secrets["client_id"],
        secrets["client_secret"],
        secrets["strings_to_exclude"],
        secrets["country_hierarchy_USA"],
        secrets["country_hierarchy_CAN"],
        secrets["cascade_API_id"],
        secrets["keyfile"],
        secrets["certfile"],
    )

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

        return temp_certfile.name, temp_keyfile.name
    
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e

def adp_bearer(client_id,client_secret,certfile,keyfile):
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

def cascade_bearer (cascade_API_id):
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

# API Calls

def api_count_adp(page_size,url,headers,type):

    api_count_params = {
            "$filter": f"workers/workAssignments/assignmentStatus/statusCode/codeValue eq '{type}'",
            "count": "true",
        }
    
    api_count_response = requests.get(url, cert=(certfile, keyfile), verify=True, headers=headers, params=api_count_params) 
    response_data = api_count_response.json()
    total_number = response_data.get("meta", {}).get("totalNumber", 0)
    api_calls = math.ceil(total_number / page_size)

    return api_calls

def api_call(page_size,skip_param,api_url,api_headers,type):
    
    api_params = {
    "$filter": f"workers/workAssignments/assignmentStatus/statusCode/codeValue eq '{type}'",
    "$top": page_size,
    "$skip": skip_param
    }

    api_response = requests.get(api_url,cert=(certfile, keyfile), headers = api_headers, params = api_params)
    time.sleep(0.6)   

    return api_response    

def api_count_cascade(api_response,page_size):
    response_data = api_response.json()
    total_number = response_data['@odata.count']
    api_calls = math.ceil(total_number / page_size)

    return api_calls

def api_call_cascade(cascade_token,api_url,api_params=None,api_data=None):
    cascade_api_headers = {
    'Authorization': f'Bearer {cascade_token}',
    }

    api_response = requests.get(api_url, headers = cascade_api_headers, params = api_params, json=api_data)
    time.sleep(0.6)   
   
    return api_response

# Global Data Calls

def status_type(status):
    status_map = {
        "active": "A",
        "terminated": "T",
        "leave": "L"
    }
    return status_map.get(status)

def GET_workers_adp():

    global adp_active,adp_terminated,adp_leave
    adp_active = []
    adp_terminated = []
    adp_leave = []

    for status in ["active","leave","terminated"]:
        print (f"       Downloading ADP Staff with the status - {status}")
               
        type = status_type (status)
        
        page_size = 100
        
        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }
        
        api_calls = api_count_adp(page_size,adp_workers,api_headers,type)
        for i in range(api_calls):
            skip_param = i * page_size

            api_response = api_call(page_size,skip_param,adp_workers,api_headers,type)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['workers']
           
                filtered_data = [
                    worker for worker in json_data 
                    if worker.get('workerID', {}).get('idValue') not in strings_to_exclude
                ]
                    
                globals()[f"adp_{status}"].extend(filtered_data)
            else:
                continue

        if Data_export:
            export_data("002 - Security and Global", f"001 - ADP (Data Out - {status}).json", globals()[f"adp_{status}"])    

    return adp_active, adp_leave, adp_terminated

def GET_workers_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")
    global cascade_responses

    cascade_responses = []

    page_size = 200

    api_response = api_call_cascade(cascade_token,cascade_workers,None)
    api_calls = api_count_cascade(api_response,page_size)                

    for i in range(api_calls):
            skip_param = i * page_size
            api_params = {
                "$top": page_size,
                "$skip": skip_param
            } 
            
            api_response = api_call_cascade(cascade_token,cascade_workers,api_params)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)    

    if Data_export:
        export_data("002 - Security and Global", "001 - Cascade Raw Out.json", cascade_responses)    

    return cascade_responses

def get_hierarchy_nodes(hierarchy_ids,url,headers):
        hierarchy_nodes = []
        hierarchy_id_nodes = []

        for h_id in hierarchy_ids:
            params = {
                "$filter": f"parentId eq '{h_id}' and disabled eq false"
            }

            for attempt in range(2):  # Attempt up to 2 times
                response = requests.get(url, params=params, headers=headers)
                time.sleep(0.6)  # Always sleep between requests

                if response.status_code == 200:
                    data = response.json()
                    for record in data.get('value', []):
                        hierarchy_nodes.append(record)
                        hierarchy_id_nodes.append(record['Id'])
                    break  # Exit retry loop on success
                elif attempt == 0:
                    print(f"            Request failed for parentId {h_id}, retrying in 1 second...")
                    time.sleep(3)
                else:
                    print(f"            Failed to retrieve data for parentId {h_id}: {response.status_code}")

        return hierarchy_nodes, hierarchy_id_nodes
    
def GET_hierarchy_list(country): 
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving Job Hierarchy Nodes (" + time_now + ")")

    H_top_level = globals()[f"country_hierarchy_{country.upper()}"]
    
    api_url = 'https://api.iris.co.uk/hr/v2/hierarchy'
    api_headers =   {
        'Authorization': f'Bearer {cascade_token}'
                    }
    
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
            new_nodes, new_id_nodes = get_hierarchy_nodes(hierarchy_id_nodes,api_url,api_headers)
            hierarchy_nodes.extend(new_nodes)
            hierarchy_id_nodes = new_id_nodes
    else:
        print(f"Failed to retrieve data: {response.status_code}")

    if Data_export:
        export_data("002 - Security and Global","002 - Hierarchy Nodes.json", hierarchy_nodes)    
            
    
    return hierarchy_nodes

def find_active_job_position(worker):
    active_job_position = None

    work_assignments = worker.get("workAssignments", [{}])
    for index, assignment in enumerate(work_assignments):
        if assignment.get("primaryIndicator", True):
            active_job_position = index
            continue
    return active_job_position

def find_cascade_id_and_cont_service(ADP_identifier):
    CascadeID = Cascade_full = contServiceCascade = None
    #print (ADP_identifier)
    for entry in cascade_responses:
        # Safely get the NationalInsuranceNumber or skip if missing
        if str(entry.get("NationalInsuranceNumber")) == str(ADP_identifier):
            #print (entry.get("NationalInsuranceNumber"))
            #print (ADP_identifier)
            CascadeID = entry.get("DisplayId")

            if CascadeID is None:
                Cascade_full = None
            else:
                Cascade_full = entry.get("Id")
                contServiceCascade = entry.get("ContinuousServiceDate")
            break  # Exit loop once a match is found
    return CascadeID,Cascade_full,contServiceCascade

def find_hierarchy_id(job_code,job_name,hierarchy_library): 
    hierarchy = None 

    # First pass: try exact match (code + name) 
    for item in hierarchy_library: 
        if str(item["adp_code"]) == str(job_code) and str(item["adp_name"]) == str(job_name): 
            hierarchy = str(item["hierarchy"])
            break 
        
        # Second pass: if no exact match, try fallback (code + name is None) 
        if hierarchy is None: 
            for item in hierarchy_library: 
                if str(item["adp_code"]) == str(job_code) and item["adp_name"] is None: 
                    hierarchy = str(item["hierarchy"])
                    break 
           
    return hierarchy

def ID_generator(country,adp_responses):

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Creating an ID library (" + time_now + ")")

    xlsx_in_memory = load_xlsx_from_bucket("Hierarchy")
    df = pd.read_excel(xlsx_in_memory, sheet_name=f"{country} Conversion")
    df = df.replace({np.nan: None})
    df['adp_code'] = df['adp_code'].astype(str)
    df['cascade_code'] = pd.to_numeric(df['cascade_code'], errors='coerce')
    hierarchy_library = df.to_dict(orient='records')
    
    ID_library = []

    if Data_export:
        export_data("002 - Security and Global",f"002 - {country} Hierarchy.json", hierarchy_library)    

    for worker in adp_responses:
        active_job_position = find_active_job_position(worker)

        id_value = worker["associateOID"]
        contServiceADP = worker["workAssignments"][0]["hireDate"]
        date_obj = datetime.strptime(contServiceADP, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%Y-%m-%dT00:00:00Z")
        ADP_identifier = worker["workAssignments"][active_job_position]["positionID"]
        LM_kzo = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID")

        job_code = None
        job_name = None
        CascadeID = None

        if country == "usa":
            job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][1]["nameCode"]["codeValue"]

        elif country == "can":
            job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][0]["nameCode"]["codeValue"]
            job_name = worker["workAssignments"][active_job_position]["jobCode"]["codeValue"]

        CascadeID, Cascade_full, contServiceCascade = find_cascade_id_and_cont_service(ADP_identifier)

        hierarchy_id = find_hierarchy_id(job_code, job_name, hierarchy_library)

        if CascadeID is None:
            date = formatted_date
            contServiceCascade = None
        else:
            date = min(contServiceCascade,formatted_date)

        transformed_record = {
            "AOID": id_value,
            "CascadeId": CascadeID if CascadeID is not None else None,
            "Cascade_full": Cascade_full if CascadeID is not None else None, 
            "ADP_number": ADP_identifier,
            "ADP_line_manager": LM_kzo,
            "Job_position": active_job_position,
            "Job Code": job_code,
            "Job Name": job_name,
            "Hierarchy": hierarchy_id,
            "Cascade Start": contServiceCascade,
            "ADP Start": formatted_date,
            "contServiceDate": date,
        }
        
        ID_library.append(transformed_record)

    if Data_export:
        export_data("002 - Security and Global","003 - ID_library.json", ID_library)
    return ID_library
    
# Cascade to ADP (run-type-1)
#---------------------------------------- Support Functions
def create_cascadeID_update_list(ID_library, cascade, adp):
    cascade_exists_in_library = False

    for record in ID_library:
        if record["CascadeId"] == cascade:
            cascade_exists_in_library = True
            break             
    
    if not cascade_exists_in_library:              
        for record in ID_library:
            if record["ADP_number"] == adp:
                cascade = record["CascadeId"]
                AOID = record["AOID"]
                break  # Added break for efficiency once match is found
        
        transformed_record = {
            "AOID": AOID,
            "Cascade": cascade
            #"Cascade": cascade if cascade is not None else ""  # Use this to strip CascadeId's out of Canadian Records
        }

        return transformed_record
#---------------------------------------- Main Functions
def whats_in_ADP(adp_responses, ID_library,c):
    ct_POST_cascade_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Finding Id's that are missing on Cascade (" + ct_POST_cascade_id + ")")

    ID_responses = []

    for worker in adp_responses:
        work_assignments = worker.get("workAssignments", [{}])
        for index, assignment in enumerate(work_assignments):
            if assignment.get("primaryIndicator", True):
                active_job_position = index
                continue
        
        ADP_number = worker["workAssignments"][active_job_position]["positionID"]       
        
        if c == "usa":
            cascade = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
        if c == "can":
            cascade = worker["customFieldGroup"]["stringFields"][0].get("stringValue", "")

        transformed_record = create_cascadeID_update_list(ID_library, cascade, ADP_number)
        if transformed_record is not None:  # Only append if we got a valid record back
            ID_responses.append(transformed_record)

    if Data_export:
        export_data("006 - CascadeId to ADP","003 - IDs_updating.json", ID_responses)    

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
#---------------------------------------- Top Level Function
def run_type_1():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Pushing Cascade Id's back to ADP (" + time_now + ")")

    CascadeId_to_upload             = whats_in_ADP(adp_responses, ID_library,c)
    upload_cascade_Ids_to_ADP(CascadeId_to_upload,c)

# Delete/Update Absences (run-type-2/5)
#---------------------------------------- Support Functions
def absence_days_from_adp(trackingID,adp_response,Cascade_full,section:int,record_no:int):
    data = adp_response

    output2= []
    uploads = []

    main = data["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"][section]["requests"][record_no]
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
               
        api_call_cascade(cascade_token,cascade_absencedays,None,new_record)

        time.sleep(1)  
                    
    if Data_export:
        export_data("005 - Absences to Cascade","absence_days.json",output2)    

#---------------------------------------- Top Level Function               
def load_from_bucket(variable):
    client = storage.Client(credentials=creds, project=project_Id)
    bucket = client.bucket("event_list_objects")
    blob = bucket.blob(f"{variable}.json")

    data = json.loads(blob.download_as_text())
    string_list = data["strings"]

    return string_list

def create_absences_reasons():
    xlsx_in_memory = load_xlsx_from_bucket("Hierarchy")
    df = pd.read_excel(xlsx_in_memory, sheet_name=f"{c} Absences")
                          
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
        export_data("005 - Absences to Cascade",f"001 - {c} absence reasons.json", absence_reasons)    
    
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
        export_data("005 - Absences to Cascade","002 - ADP Raw absence response (Individual).json", adp_response)    

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
        export_data("005 - Absences to Cascade","003 - ADP absences - categorised.json", final_output)    
        export_data("005 - Absences to Cascade","003a - ADP absences - Approved.json", filtered_extracted_records)    


    return filtered_extracted_records

def cascade_absences(Cascade_full,absences_from):

    cascade_responses = []
    api_params = {
        "$filter": "EmployeeId eq '"+Cascade_full+"' and startDate ge "+absences_from,             #add this to filter to the last 90 days
    }
    api_response = api_call_cascade(cascade_token,cascade_absences_url,api_params,None)
    
    if api_response.status_code == 200:
        json_data = api_response.json()
        cascade_responses.append(json_data)
    else:
        print(f"Failed to retrieve data from API. Status code: {api_response.status_code}")

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
        export_data("005 - Absences to Cascade","005 - Cascadecurrent.json",updated_json_data)    
        export_data("005 - Absences to Cascade","005a - Cascadecurrent_Id.json",current_absence_id_cascade)    
    
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
        export_data("005 - Absences to Cascade","006 - Unchanged.json",unchanged_records)    
        export_data("005 - Absences to Cascade","006a - Unchanged id.json",unchanged_ids)    
        export_data("005 - Absences to Cascade","007 - Update.json",update_records)    
        export_data("005 - Absences to Cascade","007a - Update (reordered).json",Update_transformed)    
        export_data("005 - Absences to Cascade","007b - Update id.json",update_ids)    
        export_data("005 - Absences to Cascade","008 - New.json",new_records)    
        export_data("005 - Absences to Cascade","008 - New.json",delete_ids)    

    return new_records, Update_transformed, delete_ids, update_ids

def POST(new_records,adp_response,Cascade_full):
    records_to_add = new_records
    output=[]

    if not records_to_add:
        print("                No records to add")
    else:
        for record in records_to_add:

            section = record["Section"]
            record_no = record["Record"]

            new_record = {
                "EmployeeId": Cascade_full,
                "AbsenceReasonId": record["AbsenceReasonId"],
                "Narrative": None,
                "StartDate": record["StartDate"],
                "EndDate": record["EndDate"],
            }

            output.append(new_record)
                                    
            response = api_call_cascade(cascade_token,cascade_absences_url,None,new_record)
            json_response = response.json()

            trackingID = json_response.get("id")
            print (f'                   {trackingID}')
                    
            if response.status_code == 200:
                json_response = response.json()
                trackingID = json_response.get("id")
            elif response.status_code == 429:
                print(f'                Failed to create absence. Rate Limit hit') 
            else:
                print("        "+f'Response Code: {response.status_code}')    
            time.sleep(0.6)  

            absence_days_from_adp(trackingID,adp_response,Cascade_full,section,record_no)

        if Data_export:
            export_data("005 - Absences to Cascade","010 - ADPabsences.json",output)    
        
    return (output)

def DELETE(delete_ids):

    if Data_export:
        export_data("005 - Absences to Cascade","011 - All deleted -ID.json",delete_ids)    

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
#---------------------------------------- Top Level Function               
def run_type_2(ID_library):

    ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
    absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    absence_reasons = create_absences_reasons()

    for record in ID_library:
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

            DELETE(delete_ids)  # Deletes cancelled absences
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

def run_type_5(ID_library):
    ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
    absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    absence_reasons = create_absences_reasons()

    for record in ID_library:
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

# Update Personal Details (Run Type 3)
#---------------------------------------Support Functions
def make_api_request(DisplayId):

    api_params = {
        "$filter": f"DisplayId eq '{DisplayId}'",
        "$select": "DisplayId,Id,ContinuousServiceDate",
    }                
    
    api_response = api_call_cascade(cascade_token,cascade_workers, api_params)
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

def load_xlsx_from_bucket(name):
    bucket_name = "event_list_objects"
    file_name = f"{name}.xlsx"

    # Reference the bucket and blob
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download file contents into memory
    data = blob.download_as_bytes()

    # Wrap in BytesIO for in-memory use
    xlsx_file = io.BytesIO(data)

    return xlsx_file

def find_active_job_position(worker):
    active_job_position = None

    work_assignments = worker.get("workAssignments", [{}])
    for index, assignment in enumerate(work_assignments):
        if assignment.get("primaryIndicator", True):
            active_job_position = index
            continue
    return active_job_position

def find_cascade_personal_data_from_ID(ADP_id, ID_library, display_id,start_date):
    contServiceSplit = start_date  # Default value if no record is found
    Id = None
   
    for entry in ID_library:
        if entry["ADP_number"] == ADP_id and entry["CascadeId"] is None:
            contService = entry["contServiceDate"]
            contServiceSplit = contService.split("T")[0]
            Id = None
            break  # Exit the loop once a match is found
        elif entry["CascadeId"] == display_id:
            contService = entry["contServiceDate"]
            contServiceSplit = contService.split("T")[0]
            Id = entry["Cascade_full"]   
            break  # Exit the loop once a match is found
    
    return contServiceSplit,Id

def convert_us_language_to_uk(workingStatus,end_date,mobileOwner):
    if workingStatus == "Active":
        workingStatus = "Current"
    if workingStatus == "Inactive":                     #This may be removed later - discussion needed AP/KG
        workingStatus = "Current"
    if workingStatus == "Terminated":
        converted_date = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        workingStatus = f"Left {converted_date}" 
    if workingStatus == "Current":                      #Override for previous termination date
        end_date = None
    
    if mobileOwner == "Personal Cell":
        mobileOwner = "Personal"
    if mobileOwner == None:
        mobileOwner = "Personal"

    return workingStatus,end_date,mobileOwner

def find_cont_service(contServiceSplit,start_date):
    employment_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    continuous_service_date = datetime.strptime(contServiceSplit, "%Y-%m-%d")
       
    if continuous_service_date > employment_start_date:
        contServiceSplit = start_date
    
    return contServiceSplit

def convert_datetime_to_date(datetime_str):
    dt_object = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
    return dt_object.strftime("%Y-%m-%d")

def create_initials(preferred,firstname,other_name):
    if preferred:
        initials = preferred[0].upper()
    else:
        initials = firstname[0].upper()

    if other_name:
        initials += " " + other_name[0].upper()
    
    return initials
#---------------------------------------Main Functions
def convert_adp_to_cascade_form(records,suffix,terminations,ID_library,x_months_ago=None):                         
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Converting the adp data to the cascade form (" + time_now+ ")")

    output = []       
        
    for worker in records:
        active_job_position = find_active_job_position(worker)
        gender = worker["person"]["genderCode"].get("shortName",None)
        salutation = worker["person"]["legalName"].get("preferredSalutations",[{}])[0].get("salutationCode",{}).get("shortName")
        firstName = worker["person"]["legalName"]["givenName"]
        preffered = worker["person"]['legalName'].get("nickName",None)
        other_name =  worker["person"]["legalName"].get("middleName")
        family_name = worker["person"]["legalName"]["familyName1"]
        if c == "usa":
            display_id = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
        elif c == "can":
            display_id = worker["customFieldGroup"]["stringFields"][0].get("stringValue")     
        workingStatus = worker["workerStatus"]["statusCode"]["codeValue"]
        isManager = worker["workAssignments"][active_job_position].get("managementPositionIndicator")
        start_date = worker["workAssignments"][active_job_position]["actualStartDate"]
        end_date = worker["workAssignments"][active_job_position].get("terminationDate")
        birthDate = worker["person"]["birthDate"]
        maritalStatus = worker["person"].get("maritalStatusCode",{}).get("shortName")
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
        workingStatus,end_date,mobileOwner = convert_us_language_to_uk(workingStatus,end_date,mobileOwner)
        contServiceSplit,Id = find_cascade_personal_data_from_ID(ADP_id,ID_library,display_id,start_date)
        contServiceSplit = find_cont_service(contServiceSplit,start_date)
        initials = create_initials(preffered,firstName,other_name)

        transformed_record = {
            "DisplayId": display_id,
            "TitleHonorific": salutation,
            "FirstName": firstName,
            "KnownAs": preffered if preffered is not None else firstName,
            "OtherName": other_name,
            "LastName": family_name,
            "CostCentre": None,
            "WorkingStatus": workingStatus,
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
            "MaritalStatus":maritalStatus,
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

        if display_id == "":
            transformed_record["Initials"] = initials

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
            export_data("003 - Personal to Cascade",f"001 - ADP_to_cascade_{suffix}.json", output)    
    
    return output

def cascade_rejig_personal(cascade_responses):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the order of Cascade to allow comparison (" + time_now + ")")

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
        export_data("003 - Personal to Cascade","002 - Cascade_reordered.json", cascade_reordered)    
    

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


    unterminated_staff = [entry for entry in adp_to_cascade_terminated 
                            if any(c.get('DisplayId') == entry.get('DisplayId') and c.get('LastWorkingDate') is None 
                            for c in cascade_reordered)]
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
            
            print(f"Leaver set - DisplayId {display_id}")
        else:
            print(f"Warning: No DisplayId found in record")
            processed_unterminated_records.append(record)

    for entry in new_starters:
        if 'Id' in entry:
            del entry['Id']
    for entry in update_personal:
        if 'Initials' in entry:
            del entry ['Initials']
    
    if Data_export:
        export_data("003 - Personal to Cascade","003a - Non Matching records.json", unique_entries)    
        export_data("003 - Personal to Cascade","003b - Updated Records.json", update_personal)    
        export_data("003 - Personal to Cascade","003c - New Starters.json", new_starters)    
        export_data("003 - Personal to Cascade","003d - Terminated Staff.json", processed_unterminated_records)    
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
                
        cascade_identifier = employee_id
    
        api_url = cascade_workers_base + "/" + cascade_identifier         

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
        time.sleep(0.6) 
    
def POST_new_starters(new_starters): 
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Adding new staff (" + time_now + ")")                              
    for entry in new_starters:
        FirstName = entry.get("FirstName")
        LastName = entry.get("LastName")
        entry["DisplayId"] = None

        transformed_record = entry
        
        if Data_export:  #Just shows the current one uploading
            export_data("003 - Personal to Cascade","005 - New Start.json", transformed_record)    

        headers = {
            'Authorization': f'Bearer {cascade_token}',
            'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=2',
            'Content-Type': 'application/json;odata.metadata=minimal;odata.streaming=true; version=2',
        }
        
        response = requests.post(cascade_workers_base, headers=headers, json=transformed_record)

        if response.status_code == 201:
            print("             " + f'New Starter Added ({FirstName} {LastName})')
        else:
            print("             "+f'Data Transfer for New Starter ({FirstName} {LastName}) has failed. Response Code: {response.status_code}')           
        time.sleep(0.6)  
#---------------------------------------- Top Level Function   
def run_type_3():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating personal details on Cascade (" + time_now + ")")

    countryName = c.upper()
    terminations = load_csv_from_bucket(f"{countryName}_termination_mapping")

    adp_to_cascade                                              = convert_adp_to_cascade_form(adp_responses,"all",terminations,ID_library)
    adp_to_cascade_terminated                                   = convert_adp_to_cascade_form(adp_terminated,"terminated",terminations,ID_library,x_months_ago)

    cascade_reordered                                           = cascade_rejig_personal(cascade_responses)
    records_to_upload, new_starters, unterminated_staff         = combine_json_files(adp_to_cascade_terminated,adp_to_cascade,cascade_reordered)
    PUT_cascade_workers_personal(records_to_upload)
    PUT_cascade_workers_personal(unterminated_staff)
    POST_new_starters(new_starters)   

# Update Job Details (Run Type 4)
#---------------------------------------Support Functions
def create_params(page_size, skip_param):
    api_params = {
        "$filter": "EndDate eq null",
        "$top": page_size,
        "$skip": skip_param
    }
    return api_params 

def find_contract(c,worker,active_job_position):
    base = worker["workAssignments"][active_job_position]
    if c == "usa":
        contract = (
            base.get("workerTypeCode", {}).get("codeValue")
            or base.get("workerGroups", [{}])[0].get("groupCode", {}).get("codeValue", "")
        )
    elif c == "can":
        contract = base.get("workerTypeCode", {}).get("codeValue", "")
    return contract

def search_ID_lib(ID_library,ADP_id):
    for record in ID_library:
        if record["ADP_number"]==ADP_id:
            employee_id = record["Cascade_full"]
            hierarchy_id = record["Hierarchy"]
            break
    return employee_id,hierarchy_id   

def find_line_manager(ID_library,LM_AOID,employee_id):
    xlsx_in_memory = load_xlsx_from_bucket("Hierarchy")
    df = pd.read_excel(xlsx_in_memory, sheet_name='JJ')
    reports_to_JJ = df['ID'].tolist()

    if employee_id in reports_to_JJ:
        line_manager = "b3775d20-8d33-4ca9-aaad-5e2346bb17e9"
    else:
        for record in ID_library:
            if record["AOID"]==LM_AOID:
                line_manager = record["Cascade_full"]   
    return line_manager

def choose_paybasis(paybasis_hourly):
    if paybasis_hourly is not None:
        paybasis = "Hourly"
    else:
        paybasis = "Yearly"
    return paybasis

def round_salary(pay_hourly,pay_annual):
    if pay_hourly is not None:
        salary = float(pay_hourly)
    else:
        salary = float(pay_annual)
    
    salary_rounded = round(salary,2)   
    return salary,salary_rounded

def change_contract_language(contract):
    permanent_codes = {"Full Time", "FT", "FTR", "FTOL", "Regular Full-Time","F"}
    if contract in permanent_codes:
        return "Permenent"
    return "Temporary"

def find_change_reason(record,salary,hierarchy_id,line_manager,startDate):
    if str(record.get("Salary")) != str(salary):
        changeReason = "Change of Salary"
    elif str(record.get("HierarchyNodeId")) != str(hierarchy_id):
        changeReason = "Change of Position"
    elif str(record.get("LineManagerId")) != str(line_manager):
        changeReason = "Change of Manager"
    elif str(record.get("StartDate")) != str(startDate):
        changeReason = "Minor Change/Correction"
    else:
        changeReason = record.get("ChangeReason")
    return changeReason

def find_start_date(effective_date_wage,cascadeStart,effective_date_other):
    wage = datetime.strptime(effective_date_wage, "%Y-%m-%d")
    cascade = datetime.strptime(cascadeStart, "%Y-%m-%d")
    other = datetime.strptime(effective_date_other, "%Y-%m-%d")

    startDate = max(wage, cascade, other)
    startDate = startDate.strftime("%Y-%m-%d")
    return startDate

def find_new_starters(records_to_add,ID_library):
    employee_ids = {record["EmployeeId"] for record in records_to_add}

    new_start_jobs = [
        {"AOID":entry["AOID"]}
        for entry in ID_library
        if entry["Cascade_full"] not in employee_ids
    ]
    return new_start_jobs

def find_name(employeeId):
    match = next((item for item in cascade_responses if item["Id"] ==employeeId),None)

    if match:
        full_name = f"{match['KnownAs']} {match['LastName']}"
    else:
        full_name = None
    return full_name

#---------------------------------------Main Functions
def cascade_current_jobs():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")
    global cascade_jobs

    page_size = 200

    api_params = create_params(page_size,0)

    api_response = api_call_cascade(cascade_token,cascade_jobs_url,api_params)
    api_calls = api_count_cascade(api_response,page_size)

    cascade_jobs = []

    for i in range(api_calls):
        skip_param = i * page_size
        api_params = create_params(page_size,skip_param)

        api_response = api_call_cascade(cascade_token,cascade_jobs_url,api_params)

        if api_response.status_code == 200:
            json_data = api_response.json()
            json_data = json_data['value']
            cascade_jobs.extend(json_data)    

    if Data_export:
        export_data("004 - Jobs to Cascade","001 - Cascade Jobs.json", cascade_jobs)    

    return cascade_jobs

def cascade_rejig_jobs(cascade_current_jobs):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the order of Cascade to allow comparison (" + time_now + ")")

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
        for entry in cascade_current_jobs
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
        export_data("004 - Jobs to Cascade","002 - Cascade_reordered.json", filtered_records)    

    return filtered_records

def adp_rejig(cascade_current,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the Jobs info to upload to Cascade (" + time_now + ")")

    transformed_records = []        
    records_to_add = []
    new_start_jobs = []

    for worker in adp_responses:
        active_job_position = find_active_job_position(worker)
       
        jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
        paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
        pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
        pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
        effective_date_wage = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
        effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")
        pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)    
        ADP_id = worker["workAssignments"][active_job_position]["positionID"]
        LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)

        contract                    = find_contract(c,worker,active_job_position)
        employee_id,hierarchy_id    = search_ID_lib(ID_library,ADP_id)
        line_manager                = find_line_manager(ID_library,LM_AOID,employee_id)
        paybasis                    = choose_paybasis(paybasis_hourly)
        salary,salary_rounded       = round_salary(pay_hourly,pay_annual)     
        contract                    = change_contract_language(contract)

        for record in cascade_current:
            if record.get("EmployeeId") == employee_id:
                Id = record.get("Id")
                jobTitle = record.get("JobTitle")
                JobFamily = record.get("JobFamily")
                cascadeStart = record.get("StartDate")
                notice = record.get("NoticePeriod")
                classification = record.get("Classification")

                effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")
                startDate = find_start_date(effective_date_wage,cascadeStart,effective_date_other)
                changeReason = find_change_reason(record,salary,hierarchy_id,line_manager,startDate)

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
                    "ChangeReason": changeReason,        
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
            
                record_to_add = {
                    "EmployeeId": employee_id,
                }

                transformed_records.append(transformed_record)
                records_to_add.append(record_to_add)
                
    new_start_jobs = find_new_starters(records_to_add,ID_library)

    if Data_export:
        export_data("004 - Jobs to Cascade","003a - ADP_reordered (Staff with roles).json", transformed_records)    
        export_data("004 - Jobs to Cascade","003b - ADP_reordered (Staff with roles - Id).json", records_to_add)   #This gives the IDs for above 
        export_data("004 - Jobs to Cascade","003c - ADP_reordered (New Starters).json",new_start_jobs)
                
    return transformed_records,new_start_jobs   

def adp_rejig_new_starters(new_starters,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the New Staff Jobs info to upload to Cascade (" + time_now + ")")
    new_start = []
    transformed_records = []        

    new_starter_values = [entry["AOID"] for entry in new_starters]

    for response in adp_responses:
        if response["associateOID"] in new_starter_values:
            new_start.append(response)
    if Data_export:
        export_data("004 - Jobs to Cascade","004d - New Starter Jobs (ADP data).json", new_start)    


    for worker in new_start:
        active_job_position = find_active_job_position(worker)
       
        jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
        paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
        pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
        pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
        hireDate = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
        pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)    
        ADP_id = worker["workAssignments"][active_job_position]["positionID"]
        LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)

        contract                    = find_contract(c,worker,active_job_position)
        employee_id,hierarchy_id    = search_ID_lib(ID_library,ADP_id)
        line_manager                = find_line_manager(ID_library,LM_AOID,employee_id)
        paybasis                    = choose_paybasis(paybasis_hourly)
        salary,salary_rounded       = round_salary(pay_hourly,pay_annual)     
        contract                    = change_contract_language(contract)

        transformed_record = {
            "JobTitle": jobTitle,
            "Classification": None,
            "StartDate": hireDate,                  
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
        export_data("004 - Jobs to Cascade","004d - New Starter Jobs.json", transformed_records)    

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
        export_data("004 - Jobs to Cascade","005a - No update needed.json", not_to_be_updated)    
        export_data("004 - Jobs to Cascade","005b - update current jobs.json", PUT_jobs)    
        export_data("004 - Jobs to Cascade","005c - add job line.json", POST_jobs)    

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
        "Id": record["Id"]}

        Id = record["Id"]
        employeeId = record["EmployeeId"]

        full_name = find_name(employeeId)

        api_base = 'https://api.iris.co.uk/hr/v2/jobs/'
        api_url = api_base + Id      

        headers = {
            'Authorization': f'Bearer {cascade_token}',
            'Content-Type':'text/json; version=2',
            'Content-Length': '22',
        }
        
        response = requests.put(api_url, headers=headers, json=update_record)
        
        if response.status_code == 204:
            print("        " + f'Current Job updated for {full_name} complete')
        else:
            print("        "+f'Data Transfer for {full_name} has failed. Response Code: {response.status_code}')           
        time.sleep(0.6)

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

            full_name = find_name(employeeId)

            api_url = 'https://api.iris.co.uk/hr/v2/jobs'

            headers = {
                'accept': 'application/json;odata.metadata=minimal;odata.streaming=true; version=2',
                'Authorization': f'Bearer {cascade_token}',
                'Content-Type':'text/json; version=2',
#                'Content-Length': '22',
            }
            
            response = requests.post(api_url, headers=headers, json=update_record)
            
            if response.status_code == 201:
                print("        " + f'New Job line added for {full_name} complete')
            else:
                print("        "+f'Data Transfer for {full_name} has failed. Response Code: {response.status_code}')           
            time.sleep(0.6)

#---------------------------------------- Top Level Function               
def run_type_4():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating Job details on Cascade (" + time_now + ")")
    
    cascade_jobs                        = cascade_current_jobs()
    cascade_current                     = cascade_rejig_jobs(cascade_jobs)
    adp_current,new_starters_jobs       = adp_rejig(cascade_current,adp_responses,ID_library)
    new_start_jobs                      = adp_rejig_new_starters(new_starters_jobs,adp_responses,ID_library)
    PUT_jobs, POST_jobs                 = classify_adp_files(new_start_jobs,adp_current,cascade_current)
    PUT_update_job_change(PUT_jobs)
    POST_create_jobs(POST_jobs, new_start_jobs)

if __name__ == "__main__":

    if testing is False:
        delete_folders()                                #clears out at the start of every run. Can be recreated if needed

    extended_update,Data_export = debug_check(debug)
    creds, project_Id = google_auth()

    x_months_ago = datetime.now() - timedelta(days=180)
    storage_client = storage.Client(credentials=creds,project=project_Id)

    def country_choice(c,run_type):
        print ("---------------------------------------------------------------------------------------------------------------")
        print (f"Synchronizing country: {c}")                                           #c represents country. Either USA or CAN

        global access_token, cascade_token, certfile, keyfile, strings_to_exclude, extended_update
        global Data_export, data_store,country_hierarchy_USA, country_hierarchy_CAN
        
        data_store = data_store_location(c)
        client_id, client_secret, strings_to_exclude, country_hierarchy_USA, country_hierarchy_CAN, cascade_API_id, keyfile, certfile = load_keys(c)
        certfile, keyfile = load_ssl(certfile, keyfile)
        access_token = adp_bearer(client_id,client_secret,certfile,keyfile)
        cascade_token = cascade_bearer (cascade_API_id)
       
        #----------     Global Data Calls     ----------#
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Making global calls (" + time_now + ")")

        global adp_responses,adp_terminated,cascade_responses,hierarchy_nodes,ID_library                              

        if testing:
            load("002 - Security and Global","001 - ADP (Data Out - active).json","adp_responses")
            load("002 - Security and Global","001 - ADP (Data Out - terminated).json","adp_terminated")
            load("002 - Security and Global","001 - Cascade Raw Out.json","cascade_responses")
            load("002 - Security and Global","002 - Hierarchy Nodes.json","hierarchy_nodes")

        else:
            adp_responses, adp_leave, adp_terminated   = GET_workers_adp()
            cascade_responses                          = GET_workers_cascade()
            hierarchy_nodes                            = GET_hierarchy_list(c)

        ID_library                                     = ID_generator(c,adp_responses)

        if run_type == 1:
            run_type_1()
        elif run_type == 2:
            run_type_2(ID_library)
        elif run_type == 3:
            run_type_3()
        elif run_type == 4:
            run_type_4()
        elif run_type == 5:
            run_type_5(ID_library)



    countries = ["usa","can"]
    #countries = ["can"]           #Use to test Country independently)

    run_type = find_run_type()
    print (f"Run type {run_type}")

    for c in countries:
        country_choice (c,run_type)

    ct_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ()
    print ("Finished - Putting up my feet (" + ct_fin + ")")
    print ("----------------------------------------------------------------------------------------------------------------")


