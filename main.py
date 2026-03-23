# Standard Library - Core
import os
import io
import sys
import json
import math
import tempfile
from pathlib import Path
import stat

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
test_time = dt_time(4,0,0)     #Testing the triggering from gcs

testing = False

current_folder = Path(__file__).resolve().parent

adp_workers             = 'https://api.adp.com/hr/v2/workers'
cascade_workers         = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
cascade_workers_base    = 'https://api.iris.co.uk/hr/v2/employees'
cascade_jobs_url        = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'
cascade_absences_url    = 'https://api.iris.co.uk/hr/v2/attendance/absences'        
cascade_absencedays     = 'https://api.iris.co.uk/hr/v2/attendance/absencedays'
adp_events_url         = 'https://api.adp.com/core/v1/event-notification-messages'

# Set up

def findRunType():
    
    now_uk = datetime.now(ZoneInfo("Europe/London"))
    is_bst = bool(now_uk.dst())

    print("Current UK time:", now_uk)
    print("BST active?", is_bst)
    
    # Get the current UK time (not system time)
    if debug is True:
        current_time = test_time
    else:
        current_time = now_uk.time()
    
    # Adjust time ranges based on BST (add 1 hour during summer)
    hour_offset = 1 if is_bst else 0
    
    # Base times (these are the winter times)
    base_time_ranges = [
        (dt_time(22, 46), dt_time(23, 15), 1),       # Push New Cascade Id's back to ADP (23:00)
        (dt_time(0, 46), dt_time(1, 15), 3),        # Updates staff personal and adds new staff (01:00)
        (dt_time(2, 45), dt_time(3, 15), 1),        # Push New Cascade Id's back to ADP (Pushes ID for new Staff) (03:00)
        (dt_time(3, 16), dt_time(3, 45), 4),        # Updates job details (03:30)
        (dt_time(3, 46), dt_time(4, 15), 2),        # Removes deleted and Adds in new and changed Absences (04:00)
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

def createFolders(current_folder, structure=None, created_paths=None):
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
            createFolders(folder_path, subfolders, created_paths)
    
    return created_paths

def deleteFolders():
    def handle_remove_readonly(func, path, exc_info):
        if isinstance(exc_info[1], PermissionError):
            # Remove readonly attribute and try again
            os.chmod(path, stat.S_IWRITE)
            time.sleep(0.1)  # Brief pause for OneDrive
            try:
                func(path)
            except PermissionError:
                print(f"Warning: Could not delete {path} - file may be locked by OneDrive")
        else:
            raise
    
    base_path_delete = current_folder / "Data Store"
    folders_to_delete = ["Data - CAN", "Data - USA"]

    for folder_name in folders_to_delete:
        # Get the folder path
        folder_path = base_path_delete / folder_name
        
        # Delete the folder and its contents recursively if it exists
        if os.path.exists(folder_path):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    shutil.rmtree(folder_path, onexc=handle_remove_readonly)
                    print(f"Successfully deleted: {folder_path}")
                    break
                except PermissionError as e:
                    if attempt < max_retries - 1:
                        print(f"Retry {attempt + 1}/{max_retries} for {folder_path}")
                        time.sleep(1)
                    else:
                        print(f"Failed to delete {folder_path} after {max_retries} attempts: {e}")
                        print("Continuing with script execution...")
                except Exception as e:
                    print(f"Unexpected error deleting {folder_path}: {e}")
                    break
    
    time.sleep(1)

def exportData(folder, filename, variable):
    if data_export:
        file_path = Path(data_store) / folder / filename
        with open(file_path, "w") as outfile:
            json.dump(variable, outfile, indent=4)

def load(folder,filename,variable_name):
    file_path = Path(data_store) / folder / filename
    with open(file_path,"r") as file:
        globals()[variable_name] = json.load(file)

def googleAuth():
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

def debugCheck(debug):
    if debug:
        if testing is False:
            createFolders(current_folder)     
        extended_update = False                                          
        data_export = True
    
    else:
        extended_update = True                                                            
        data_export = False
                    
    return extended_update,data_export

def dataStoreLocation(country):
    folder_name = f"Data - {country.upper()}"
    return os.path.join(current_folder, "Data Store", folder_name)

def getSecret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_Id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def loadKeys(country):
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

    secrets = {k: getSecret(v) for k, v in secret_ids.items()}

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

def loadSsl(certfile_content, keyfile_content):
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

def adpBearer(client_id,client_secret,certfile,keyfile):
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

def cascadeBearer (cascade_API_id):
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

def apiCountAdp(page_size,url,headers,type):

    api_count_params = {
            "$filter": f"workers/workAssignments/assignmentStatus/statusCode/codeValue eq '{type}'",
            "count": "true",
        }
    
    api_count_response = requests.get(url, cert=(certfile, keyfile), verify=True, headers=headers, params=api_count_params) 
    response_data = api_count_response.json()
    total_number = response_data.get("meta", {}).get("totalNumber", 0)
    api_calls = math.ceil(total_number / page_size)

    return api_calls

def apiCall(page_size,skip_param,api_url,api_headers,type):
    
    api_params = {
    "$filter": f"workers/workAssignments/assignmentStatus/statusCode/codeValue eq '{type}'",
    "$top": page_size,
    "$skip": skip_param
    }

    api_response = requests.get(api_url,cert=(certfile, keyfile), headers = api_headers, params = api_params)
    time.sleep(0.6)   

    return api_response    

def apiCountCascade(api_response,page_size):
    response_data = api_response.json()
    total_number = response_data['@odata.count']
    api_calls = math.ceil(total_number / page_size)

    return api_calls

def apiCallCascade(cascade_token,api_url,api_params=None,api_data=None):
    cascade_api_headers = {
    'Authorization': f'Bearer {cascade_token}',
    }

    api_response = requests.get(api_url, headers = cascade_api_headers, params = api_params, json=api_data)
    time.sleep(0.6)   
   
    return api_response

def apiCallEvents(page_size,skip_param,api_url,api_headers):
    
    api_params = {
    "$top": page_size,
    "$skip": skip_param
    }

    api_response = requests.get(api_url,cert=(certfile, keyfile), headers = api_headers, params = api_params)
    time.sleep(0.6)   

    return api_response    

# Global Data Calls

def statusType(status):
    status_map = {
        "active": "A",
        "terminated": "T",
        "leave": "L"
    }
    return status_map.get(status)

def getWorkersAdp():

    global adp_active,adp_terminated,adp_leave
    adp_active = []
    adp_terminated = []
    adp_leave = []

    for status in ["active","leave","terminated"]:
        print (f"       Downloading ADP Staff with the status - {status}")
               
        type = statusType (status)
        
        page_size = 100
        
        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept':"application/json;masked=false"
            }
        
        api_calls = apiCountAdp(page_size,adp_workers,api_headers,type)
        for i in range(api_calls):
            skip_param = i * page_size

            api_response = apiCall(page_size,skip_param,adp_workers,api_headers,type)

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

        exportData("002 - Security and Global", f"001 - ADP (Data Out - {status}).json", globals()[f"adp_{status}"])    

    return adp_active, adp_leave, adp_terminated

def getWorkersCascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")
    global cascade_responses

    cascade_responses = []

    page_size = 200

    api_response = apiCallCascade(cascade_token,cascade_workers,None)
    api_calls = apiCountCascade(api_response,page_size)                

    for i in range(api_calls):
            skip_param = i * page_size
            api_params = {
                "$top": page_size,
                "$skip": skip_param
            } 
            
            api_response = apiCallCascade(cascade_token,cascade_workers,api_params)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)    

    cascade_responses = [record for record in cascade_responses if record.get('DisplayId') is not None]

    exportData("002 - Security and Global", "001 - Cascade Raw Out.json", cascade_responses)    

    return cascade_responses

def getHierarchyNodes(hierarchy_ids,url,headers):
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
    
def getHierarchyList(country): 
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
            new_nodes, new_id_nodes = getHierarchyNodes(hierarchy_id_nodes,api_url,api_headers)
            hierarchy_nodes.extend(new_nodes)
            hierarchy_id_nodes = new_id_nodes
    else:
        print(f"Failed to retrieve data: {response.status_code}")

    exportData("002 - Security and Global","002 - Hierarchy Nodes.json", hierarchy_nodes)    
            
    
    return hierarchy_nodes

def findActiveJobPosition(worker):
    active_job_position = None

    work_assignments = worker.get("workAssignments", [{}])
    for index, assignment in enumerate(work_assignments):
        if assignment.get("primaryIndicator", True):
            active_job_position = index
            continue
    return active_job_position

def findCascadeIdAndContService(ADP_identifier):
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

def findHierarchyId(job_code,job_name,hierarchy_library): 
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

def IDGenerator(country,adp_responses):

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Creating an ID library (" + time_now + ")")

    xlsx_in_memory = loadXlsxFromBucket("Hierarchy")
    df = pd.read_excel(xlsx_in_memory, sheet_name=f"{country} Conversion")
    df = df.replace({np.nan: None})
    df['adp_code'] = df['adp_code'].astype(str)
    df['cascade_code'] = pd.to_numeric(df['cascade_code'], errors='coerce')
    hierarchy_library = df.to_dict(orient='records')
    
    ID_library = []

    exportData("002 - Security and Global",f"002 - {country} Hierarchy.json", hierarchy_library)    

    for worker in adp_responses:
        active_job_position = findActiveJobPosition(worker)

        id_value = worker["associateOID"]
        contServiceADP = worker["workAssignments"][0]["hireDate"]
        date_obj = datetime.strptime(contServiceADP, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%Y-%m-%dT00:00:00Z")
        ADP_identifier = worker["workAssignments"][active_job_position]["positionID"]
        LM_kzo = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID")

        job_code = None
        job_name = None
        CascadeID = None

        job_name = worker["workAssignments"][active_job_position]["jobCode"].get("codeValue","")


        if country == "usa":
            job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][1]["nameCode"]["codeValue"]

        elif country == "can":
            job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][0]["nameCode"]["codeValue"]
            #job_name = worker["workAssignments"][active_job_position]["jobCode"]["codeValue"]

        CascadeID, Cascade_full, contServiceCascade = findCascadeIdAndContService(ADP_identifier)

        hierarchy_id = findHierarchyId(job_code, job_name, hierarchy_library)

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

    exportData("002 - Security and Global","003 - ID_library.json", ID_library)
    
    return ID_library
    
# Cascade to ADP (run-type-1)
#---------------------------------------- Support Functions
def createCascadeidUpdateList(ID_library, cascade, adp):
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
def whatsInAdp(adp_responses, ID_library,c):
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

        transformed_record = createCascadeidUpdateList(ID_library, cascade, ADP_number)
        if transformed_record is not None:  # Only append if we got a valid record back
            ID_responses.append(transformed_record)

    exportData("006 - CascadeId to ADP","003 - IDs_updating.json", ID_responses)    

    return ID_responses

def uploadCascadeidsToAdp(CascadeId_to_upload,country):
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
def runType1():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Pushing Cascade Id's back to ADP (" + time_now + ")")

    CascadeId_to_upload             = whatsInAdp(adp_responses, ID_library,c)
    uploadCascadeidsToAdp(CascadeId_to_upload,c)

# Delete/Update Absences (run-type-2)
#---------------------------------------- Top Level Function               
def loadFromBucket(variable):
    client = storage.Client(credentials=creds, project=project_Id)
    bucket = client.bucket("event_list_objects")
    blob = bucket.blob(f"{variable}.json")

    data = json.loads(blob.download_as_text())
    string_list = data["strings"]

    return string_list

def createAbsencesReasons():
    xlsx_in_memory = loadXlsxFromBucket("Hierarchy")
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
    
    exportData("005 - Absences to Cascade",f"001 - {c} absence reasons.json", absence_reasons)    
    
    return absence_reasons

def getCascadeId(CascadeId,ID_library):
    for record in ID_library:
        if record["CascadeId"] == CascadeId:
            Cascade_full = record["Cascade_full"]
            AOID = record["AOID"]
            break
    
    return Cascade_full,AOID

def getAbsencesAdp(AOID):
    api_url = "https://api.adp.com/time/v2/workers/" + AOID + "/time-off-details/time-off-requests"
    api_headers = {
        'Authorization': f'Bearer {access_token}',
    }

    while True:
        api_response = requests.get(api_url, cert=(certfile, keyfile), verify=True, headers=api_headers)
        
        if api_response.status_code == 429:
            time.sleep(1)
            continue
        
        break
    
    adp_response = api_response.json()

    exportData("005 - Absences to Cascade","002 - ADP Raw absence response (Individual).json", adp_response)    

    return adp_response

def convertAdpAbsencesToCascadeFormat(adp_response, absence_reasons, Cascade_full, AOID, ninety_days_ago):
    requests = adp_response["paidTimeOffDetails"]["paidTimeOffRequests"][0]["paidTimeOffRequestEntries"]
    approved_records = []

    for section in requests:
        if section["requestStatus"]["labelName"] != "Approved":
            continue

        for x, main in enumerate(section["requests"]):
            absence_policy = main["paidTimeOffEntries"][0]["paidTimeOffPolicy"].get("labelName", "")

            if c == "usa":
                absence_earning_type = main["paidTimeOffEntries"][0]["earningType"].get("labelName", "")
                AbsenceReasonId = next(
                    (a["cascadeAbsenceId"] for a in absence_reasons
                     if a["policy"] == absence_policy and a["earningType"] == absence_earning_type),
                    None
                )
            elif c == "can":
                AbsenceReasonId = next(
                    (a["Id"] for a in absence_reasons if a["Name"] == absence_policy),
                    None
                )

            approved_records.append({
                "isFromSync": False,
                "trackingId": None,
                "EmployeeId": Cascade_full,
                "AbsenceReasonId": AbsenceReasonId,
                "Narrative": None,
                "StartDate": main["paidTimeOffEntries"][0]["timePeriod"]["startDateTime"],
                "EndDate": main["paidTimeOffEntries"][-1]["timePeriod"]["endDateTime"],
            })

    filtered_records = [
        r for r in approved_records
        if datetime.strptime(r["StartDate"], "%Y-%m-%d") >= ninety_days_ago
    ]

    exportData("005 - Absences to Cascade", "003a - ADP absences - Approved.json", filtered_records)

    return filtered_records

def cascadeAbsences(Cascade_full,absences_from):

    cascade_responses = []
    api_params = {
        "$filter": "EmployeeId eq '"+Cascade_full+"' and startDate ge "+absences_from,             #add this to filter to the last 90 days
    }
    api_response = apiCallCascade(cascade_token,cascade_absences_url,api_params,None)
    
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
            "StartDate": convertDatetimeToDate(entry["StartDate"]),
            "EndDate": convertDatetimeToDate(entry["EndDate"]),
        }
        for entry in combined_data
    ]

    current_absence_id_cascade = [entry["id"] for entry in updated_json_data]

    exportData("005 - Absences to Cascade","005 - Cascadecurrent.json",updated_json_data)    
    exportData("005 - Absences to Cascade","005a - Cascadecurrent_Id.json",current_absence_id_cascade)    
    
    return  updated_json_data,current_absence_id_cascade

def combineJsonFilesForPost(current_absence_id_cascade,adp_current,cascade_current):
    cascade_index = {
        (r['StartDate'], r['EndDate'], r['AbsenceReasonId']): r
        for r in cascade_current
    }
    cascade_partial = {}  # For partial matches (updates)
    for r in cascade_current:
        for key in [(r['StartDate'], r['EndDate']), (r['StartDate'], r['AbsenceReasonId'])]:
            cascade_partial.setdefault(key, r)

    unchanged_ids, update_ids, processed_records = [], [], set()
    Update_transformed, unchanged_records, update_records = [], [], []

    for adp in adp_current:
        exact_key = (adp['StartDate'], adp['EndDate'], adp['AbsenceReasonId'])
        if exact_key in cascade_index:
            unchanged_records.append(adp)
            unchanged_ids.append(cascade_index[exact_key]['id'])
            processed_records.add((adp['EmployeeId'], adp['StartDate'], adp['EndDate']))
        else:
            # Check partial match for updates
            partial_key = (adp['StartDate'], adp['EndDate'])
            if partial_key in cascade_partial:
                match = cascade_partial[partial_key]
                update_records.append(adp)
                update_ids.append(match['id'])
                processed_records.add((adp['EmployeeId'], adp['StartDate'], adp['EndDate']))

    new_records = [r for r in adp_current
                   if (r['EmployeeId'], r['StartDate'], r['EndDate']) not in processed_records]

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

    exportData("005 - Absences to Cascade","006 - Unchanged.json",unchanged_records)    
    exportData("005 - Absences to Cascade","006a - Unchanged id.json",unchanged_ids)    
    exportData("005 - Absences to Cascade","007 - Update.json",update_records)    
    exportData("005 - Absences to Cascade","007a - Update (reordered).json",Update_transformed)    
    exportData("005 - Absences to Cascade","007b - Update id.json",update_ids)    
    exportData("005 - Absences to Cascade","008 - New.json",new_records)    
    exportData("005 - Absences to Cascade","008 - New.json",delete_ids)    

    return new_records, Update_transformed, delete_ids, update_ids

def PostAbsences(new_records,Cascade_full):
    output=[]

    if not new_records:
        print("                No records to add")
    else:
        for record in new_records:

            new_record = {
                "EmployeeId": Cascade_full,
                "AbsenceReasonId": record["AbsenceReasonId"],
                "Narrative": None,
                "StartDate": record["StartDate"],
                "EndDate": record["EndDate"],
            }

            params = {
                "autoGenerateDaysBasedOnWorkingPattern": "true"
            }

            headers = {
                "accept": "application/json;odata.metadata=minimal;odata.streaming=true; version=1",
                "Authorization": f"Bearer {cascade_token}",
                "Content-Type": "application/json;odata.metadata=minimal;odata.streaming=true; version=1"
            }

            response = requests.post(cascade_absences_url, params=params, headers=headers, json=new_record)

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
            time.sleep(0.2)

        exportData("005 - Absences to Cascade","010 - ADPabsences.json",output)    
        
    return (output)

def DeleteAbsences(delete_ids):

    exportData("005 - Absences to Cascade","011 - All deleted -ID.json",delete_ids)    

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
def DeleteEventNotification(id):
    delete_url = adp_events_url + "/" + id

    api_headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'roleCode': 'employee'
    }

    requests.delete(delete_url, cert=(certfile, keyfile), headers=api_headers)

def GetEventsAdp():
    page_size = 100

    api_headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': "*/*;masked=false"
    }

    api_response = apiCallEvents(page_size, "0", adp_events_url, api_headers)

    if api_response.status_code != 200:
        print(f"API returned error: {api_response.status_code}")
        return None

    adp_msg_id = api_response.headers.get("adp-msg-msgid")
    
    if not adp_msg_id:
        print("No adp_msg_id found in headers, stopping.")
        return None
    
    data = api_response.json()
    events = data.get("events", [])

    if not events:
        print("No events returned, stopping.")
        return None

    associate_oid = data["events"][0]["actor"]["associateOID"]

    if not associate_oid:
        print("No associateOID found, stopping.")
        return None

    DeleteEventNotification(adp_msg_id)

    return associate_oid  # Return the value instead of recursing

def FindEventAoid():
    print(f"       Downloading event Notifications")
    associate_oid_list = []

    while True:
        oid = GetEventsAdp()

        if oid is None:
            print(f"Stopping. Collected {len(associate_oid_list)} OIDs.")
            break

        associate_oid_list.append(oid)
    
    return associate_oid_list

#---------------------------------------- Top Level Function               
def runType2(ID_library):

    ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
    absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    absence_reasons = createAbsencesReasons()

    filtered_id_library = ID_library

    if c == "usa" and datetime.today().weekday() < 5:        
        print (len(ID_library))
        filter = FindEventAoid()
        associate_oid_list = list(set(filter))
        filtered_id_library = [entry for entry in ID_library if entry["AOID"] in associate_oid_list]
        print (len(filtered_id_library))


    for record in filtered_id_library:
        CascadeId = record["CascadeId"]
        print(f"Updating absences for {CascadeId}")
        Cascade_full, AOID = getCascadeId(CascadeId,ID_library)            
        
        try:
            adp_response = getAbsencesAdp(AOID)                               #Downloads the absences in the last 90 days for a given staff member

            if len(adp_response) == 0:
                print(f"        No booked absences for {CascadeId}")
                continue  # If there are no absences, skip to the next record                
            else:
                adp_current = convertAdpAbsencesToCascadeFormat(adp_response,absence_reasons,Cascade_full,AOID,ninety_days_ago)                             # Converts ADP absences into Cascade format
                cascade_current, current_absence_id_cascade = cascadeAbsences(Cascade_full,absences_from)                                                   # Pulls list of current absences
                new_records, Update_transformed, delete_ids, update_ids = combineJsonFilesForPost(current_absence_id_cascade,adp_current,cascade_current)   # Compares adp and cascade and removes any that are already in cascade

            DeleteAbsences(delete_ids)  # Deletes cancelled absences
            PostAbsences(new_records,Cascade_full)  # Creates new absences

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
def makeApiRequest(DisplayId):

    api_params = {
        "$filter": f"DisplayId eq '{DisplayId}'",
        "$select": "DisplayId,Id,ContinuousServiceDate",
    }                
    
    api_response = apiCallCascade(cascade_token,cascade_workers, api_params)
    response_data = api_response.json()       
    cascade_id_full = response_data['value'][0]['Id']
    cont_service_raw = response_data['value'][0]['ContinuousServiceDate']
    cont_service = datetime.fromisoformat(cont_service_raw.replace("Z", "")).strftime('%Y-%m-%d')

    time.sleep(0.6)                                               #forces a wait to eliminate 429 errors
    return cascade_id_full, cont_service

def loadCsvFromBucket(name):
    bucket_name = "event_list_objects"
    file_name = f"{name}.csv"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        raise FileNotFoundError(f"The file {file_name} does not exist in bucket {bucket_name}.")

    csv_data = blob.download_as_text(encoding="utf-8-sig")
    csv_reader = csv.DictReader(StringIO(csv_data), delimiter=',')

    result = [row for row in csv_reader]

    exportData("003 - Personal to Cascade","000 - terminations.json",result)

    return result

def loadXlsxFromBucket(name):
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

def findCascadePersonalDataFromId(ADP_id, ID_library, display_id,start_date):
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

def convertUsaTerminologyToUkTerminology(workingStatus,end_date,mobileOwner):
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

def findContService(contServiceSplit,start_date):
    employment_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    continuous_service_date = datetime.strptime(contServiceSplit, "%Y-%m-%d")
       
    if continuous_service_date > employment_start_date:
        contServiceSplit = start_date
    
    return contServiceSplit

def convertDatetimeToDate(datetime_str):
    dt_object = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
    return dt_object.strftime("%Y-%m-%d")

def createInitials(preferred,firstname,other_name):
    if preferred:
        initials = preferred[0].upper()
    else:
        initials = firstname[0].upper()

    if other_name:
        initials += " " + other_name[0].upper()
    
    return initials
#---------------------------------------Main Functions
def convertAdpToCascadeForm(records,suffix,terminations,ID_library,x_months_ago=None):                         
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Converting the adp data to the cascade form (" + time_now+ ")")

    output = []       
        
    for worker in records:
        active_job_position = findActiveJobPosition(worker)
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
        workingStatus,end_date,mobileOwner = convertUsaTerminologyToUkTerminology(workingStatus,end_date,mobileOwner)
        contServiceSplit,Id = findCascadePersonalDataFromId(ADP_id,ID_library,display_id,start_date)
        contServiceSplit = findContService(contServiceSplit,start_date)
        initials = createInitials(preffered,firstName,other_name)

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
        exportData("003 - Personal to Cascade",f"001 - ADP_to_cascade_{suffix}.json", output)    
    
    return output

def cascadeRejigPersonal(cascade_responses):
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
                "EmploymentStartDate": convertDatetimeToDate(entry["EmploymentStartDate"]) if entry["EmploymentStartDate"] is not None else None,
                "EmploymentLeftDate": convertDatetimeToDate(entry["EmploymentLeftDate"]) if entry["EmploymentLeftDate"] is not None else None,
                "ContinuousServiceDate": convertDatetimeToDate(entry["ContinuousServiceDate"]) if entry["ContinuousServiceDate"] is not None else None,
                "DateOfBirth": convertDatetimeToDate(entry["DateOfBirth"]) if entry["DateOfBirth"] is not None else None,
                "LastWorkingDate": convertDatetimeToDate(entry["LastWorkingDate"]) if entry["LastWorkingDate"] is not None else None,
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

    exportData("003 - Personal to Cascade","002 - Cascade_reordered.json", cascade_reordered)    
    
    return cascade_reordered

def combineJsonFiles(adp_to_cascade_terminated,adp_to_cascade,cascade_reordered):
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
            new_id,cont_service = makeApiRequest(display_id)
            
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
    
    exportData("003 - Personal to Cascade","003a - Non Matching records.json", unique_entries)    
    exportData("003 - Personal to Cascade","003b - Updated Records.json", update_personal)    
    exportData("003 - Personal to Cascade","003c - New Starters.json", new_starters)    
    exportData("003 - Personal to Cascade","003d - Terminated Staff.json", processed_unterminated_records)    

    return update_personal, new_starters, processed_unterminated_records

def PutCascadeWorkersPersonal(list_of_staff):
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
    
def PostNewStarters(new_starters): 
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Adding new staff (" + time_now + ")")                              
    for entry in new_starters:
        FirstName = entry.get("FirstName")
        LastName = entry.get("LastName")
        entry["DisplayId"] = None

        transformed_record = entry
        
        exportData("003 - Personal to Cascade","005 - New Start.json", transformed_record)    

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
def runType3():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating personal details on Cascade (" + time_now + ")")

    countryName = c.upper()
    terminations = loadCsvFromBucket(f"{countryName}_termination_mapping")

    adp_to_cascade                                              = convertAdpToCascadeForm(adp_responses,"all",terminations,ID_library)
    adp_to_cascade_terminated                                   = convertAdpToCascadeForm(adp_terminated,"terminated",terminations,ID_library,x_months_ago)

    cascade_reordered                                           = cascadeRejigPersonal(cascade_responses)
    records_to_upload, new_starters, unterminated_staff         = combineJsonFiles(adp_to_cascade_terminated,adp_to_cascade,cascade_reordered)
    PutCascadeWorkersPersonal(records_to_upload)
    PutCascadeWorkersPersonal(unterminated_staff)
    PostNewStarters(new_starters)   

# Update Job Details (Run Type 4)
#---------------------------------------Support Functions
def createParams(page_size, skip_param):
    api_params = {
        "$filter": "EndDate eq null",
        "$top": page_size,
        "$skip": skip_param
    }
    return api_params 

def findContract(c,worker,active_job_position):
    base = worker["workAssignments"][active_job_position]
    if c == "usa":
        contract = (
            base.get("workerTypeCode", {}).get("codeValue")
            or base.get("workerGroups", [{}])[0].get("groupCode", {}).get("codeValue", "")
        )
    elif c == "can":
        contract = base.get("workerTypeCode", {}).get("codeValue", "")
    return contract

def searchIdLibrary(ID_library,ADP_id):
    for record in ID_library:
        if record["ADP_number"]==ADP_id:
            employee_id = record["Cascade_full"]
            hierarchy_id = record["Hierarchy"]
            break
    return employee_id,hierarchy_id   

def findLineManager(ID_library,LM_AOID,employee_id):
    xlsx_in_memory = loadXlsxFromBucket("Hierarchy")
    df = pd.read_excel(xlsx_in_memory, sheet_name='JJ')
    reports_to_JJ = df['ID'].tolist()

    if employee_id in reports_to_JJ:                            #JJ
        line_manager = "b3775d20-8d33-4ca9-aaad-5e2346bb17e9"
    if LM_AOID == "G3BFJBFXG2J1KB05":
        line_manager = "6f3f3e39-f6cb-4dfe-94d8-688a17ac092c"
    else:
        for record in ID_library:
            if record["AOID"]==LM_AOID:
                line_manager = record["Cascade_full"]   
    return line_manager

def choosePaybasis(paybasis_hourly):
    if paybasis_hourly is not None:
        paybasis = "Hourly"
    else:
        paybasis = "Yearly"
    return paybasis

def roundSalary(pay_hourly,pay_annual):
    if pay_hourly is not None:
        salary = float(pay_hourly)
    else:
        salary = float(pay_annual)
    
    salary_rounded = round(salary,2)   
    return salary,salary_rounded

def changeContractLanguage(contract):
    permanent_codes = {"Full Time", "FT", "FTR", "FTOL", "Regular Full-Time","F"}
    if contract in permanent_codes:
        return "Permenent"
    return "Temporary"

def findChangeReason(record,salary,hierarchy_id,line_manager,startDate):
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

def findStartDate(effective_date_wage,cascadeStart,effective_date_other):
    wage = datetime.strptime(effective_date_wage, "%Y-%m-%d")
    cascade = datetime.strptime(cascadeStart, "%Y-%m-%d")
    other = datetime.strptime(effective_date_other, "%Y-%m-%d")

    startDate = max(wage, cascade, other)
    startDate = startDate.strftime("%Y-%m-%d")
    return startDate

def findNewStarters(records_to_add,ID_library):
    employee_ids = {record["EmployeeId"] for record in records_to_add}

    new_start_jobs = [
        {"AOID":entry["AOID"]}
        for entry in ID_library
        if entry["Cascade_full"] not in employee_ids
    ]
    return new_start_jobs

def findName(employeeId):
    match = next((item for item in cascade_responses if item["Id"] ==employeeId),None)

    if match:
        full_name = f"{match['KnownAs']} {match['LastName']}"
    else:
        full_name = None
    return full_name

#---------------------------------------Main Functions
def cascadeCurrentJobs():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")
    global cascade_jobs

    page_size = 200

    api_params = createParams(page_size,0)

    api_response = apiCallCascade(cascade_token,cascade_jobs_url,api_params)
    api_calls = apiCountCascade(api_response,page_size)

    cascade_jobs = []

    for i in range(api_calls):
        skip_param = i * page_size
        api_params = createParams(page_size,skip_param)

        api_response = apiCallCascade(cascade_token,cascade_jobs_url,api_params)

        if api_response.status_code == 200:
            json_data = api_response.json()
            json_data = json_data['value']
            cascade_jobs.extend(json_data)    

    exportData("004 - Jobs to Cascade","001 - Cascade Jobs.json", cascade_jobs)    

    return cascade_jobs

def cascadeRejigJobs(cascade_current_jobs):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the order of Cascade to allow comparison (" + time_now + ")")

    cascade_reordered = [
            {
            "JobTitle": entry.get("JobTitle"),
            "Classification": entry.get("Classification"),
            "StartDate": convertDatetimeToDate(entry["StartDate"]) if entry["StartDate"] is not None else None,
            "EndDate": convertDatetimeToDate(entry["EndDate"]) if entry["EndDate"] is not None else None,
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
            "ApprenticeshipEndDate": convertDatetimeToDate(entry["ApprenticeshipEndDate"]) if entry["ApprenticeshipEndDate"] is not None else None,
            "ContractEndDate": convertDatetimeToDate(entry["ContractEndDate"]) if entry["ContractEndDate"] is not None else None,
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

    exportData("004 - Jobs to Cascade","002 - Cascade_reordered.json", filtered_records)    

    return filtered_records

def adpRejig(cascade_current,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the Jobs info to upload to Cascade (" + time_now + ")")

    transformed_records = []        
    records_to_add = []
    new_start_jobs = []

    for worker in adp_responses:
        active_job_position = findActiveJobPosition(worker)
    
        jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
        paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
        pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
        pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
        effective_date_wage = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
        effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")
        pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)    
        ADP_id = worker["workAssignments"][active_job_position]["positionID"]
        LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)

        contract                    = findContract(c,worker,active_job_position)
        employee_id,hierarchy_id    = searchIdLibrary(ID_library,ADP_id)
        try:
            line_manager                = findLineManager(ID_library,LM_AOID,employee_id)
        except Exception:
            print (ADP_id)
            print (LM_AOID)
        paybasis                    = choosePaybasis(paybasis_hourly)
        salary,salary_rounded       = roundSalary(pay_hourly,pay_annual)     
        contract                    = changeContractLanguage(contract)

        for record in cascade_current:
            if record.get("EmployeeId") == employee_id:
                Id = record.get("Id")
                jobTitle = record.get("JobTitle")
                JobFamily = record.get("JobFamily")
                cascadeStart = record.get("StartDate")
                notice = record.get("NoticePeriod")
                classification = record.get("Classification")

                effective_date_other = worker.get("workAssignments", [{}])[active_job_position].get("assignmentStatus", {}).get("effectiveDate")
                startDate = findStartDate(effective_date_wage,cascadeStart,effective_date_other)
                changeReason = findChangeReason(record,salary,hierarchy_id,line_manager,startDate)

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

    new_start_jobs = findNewStarters(records_to_add,ID_library)

    exportData("004 - Jobs to Cascade","003a - ADP_reordered (Staff with roles).json", transformed_records)    
    exportData("004 - Jobs to Cascade","003b - ADP_reordered (Staff with roles - Id).json", records_to_add)   #This gives the IDs for above 
    exportData("004 - Jobs to Cascade","003c - ADP_reordered (New Starters).json",new_start_jobs)
                
    return transformed_records,new_start_jobs   

def adpRejigNewStarters(new_starters,adp_responses,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("        Faffling about with the New Staff Jobs info to upload to Cascade (" + time_now + ")")
    new_start = []
    transformed_records = []        

    new_starter_values = [entry["AOID"] for entry in new_starters]

    for response in adp_responses:
        if response["associateOID"] in new_starter_values:
            new_start.append(response)
    exportData("004 - Jobs to Cascade","004d - New Starter Jobs (ADP data).json", new_start)    


    for worker in new_start:
        active_job_position = findActiveJobPosition(worker)
       
        jobTitle = worker["workAssignments"][active_job_position].get("jobTitle")
        paybasis_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("nameCode", {}).get("shortName", None)
        pay_hourly = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("hourlyRateAmount", {}).get("amountValue", None)
        pay_annual = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("annualRateAmount", {}).get("amountValue", None)
        hireDate = worker.get("workAssignments", [{}])[active_job_position].get("baseRemuneration", {}).get("effectiveDate")
        pay_frequency = worker.get("workAssignments", [{}])[active_job_position].get("payCycleCode", {}).get("shortName", None)    
        ADP_id = worker["workAssignments"][active_job_position]["positionID"]
        LM_AOID = worker['workAssignments'][active_job_position].get('reportsTo',[{}])[0].get("associateOID",None)

        contract                    = findContract(c,worker,active_job_position)
        employee_id,hierarchy_id    = searchIdLibrary(ID_library,ADP_id)
        line_manager                = findLineManager(ID_library,LM_AOID,employee_id)
        paybasis                    = choosePaybasis(paybasis_hourly)
        salary,salary_rounded       = roundSalary(pay_hourly,pay_annual)     
        contract                    = changeContractLanguage(contract)

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

    exportData("004 - Jobs to Cascade","004d - New Starter Jobs.json", transformed_records)    

    return transformed_records
                        
def classifyAdpFiles(new_start_jobs,adp_current,cascade_current):
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

    exportData("004 - Jobs to Cascade","005a - No update needed.json", not_to_be_updated)    
    exportData("004 - Jobs to Cascade","005b - update current jobs.json", PUT_jobs)    
    exportData("004 - Jobs to Cascade","005c - add job line.json", POST_jobs)    

    return PUT_jobs, POST_jobs

def PutUpdateJobChange(PUT_jobs):
    
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

        full_name = findName(employeeId)

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

def PostCreateJobs(POST_jobs, new_start_jobs):
                
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

            full_name = findName(employeeId)

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
    
    cascade_jobs                        = cascadeCurrentJobs()
    cascade_current                     = cascadeRejigJobs(cascade_jobs)
    adp_current,new_starters_jobs       = adpRejig(cascade_current,adp_responses,ID_library)
    new_start_jobs                      = adpRejigNewStarters(new_starters_jobs,adp_responses,ID_library)
    PUT_jobs, POST_jobs                 = classifyAdpFiles(new_start_jobs,adp_current,cascade_current)
    PutUpdateJobChange(PUT_jobs)
    PostCreateJobs(POST_jobs, new_start_jobs)

# Main Function

if __name__ == "__main__":

    if testing is False:
        deleteFolders()                                #clears out at the start of every run. Can be recreated if needed

    extended_update,data_export = debugCheck(debug)
    creds, project_Id = googleAuth()

    x_months_ago = datetime.now() - timedelta(days=180)
    storage_client = storage.Client(credentials=creds,project=project_Id)

    def country_choice(c,run_type):
        print ("---------------------------------------------------------------------------------------------------------------")
        print (f"Synchronizing country: {c}")                                           #c represents country. Either USA or CAN

        global access_token, cascade_token, certfile, keyfile, strings_to_exclude, extended_update
        global Data_export, data_store,country_hierarchy_USA, country_hierarchy_CAN
        
        data_store = dataStoreLocation(c)
        client_id, client_secret, strings_to_exclude, country_hierarchy_USA, country_hierarchy_CAN, cascade_API_id, keyfile, certfile = loadKeys(c)
        certfile, keyfile = loadSsl(certfile, keyfile)
        access_token = adpBearer(client_id,client_secret,certfile,keyfile)
        cascade_token = cascadeBearer (cascade_API_id)
       
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
            adp_responses, adp_leave, adp_terminated   = getWorkersAdp()
            cascade_responses                          = getWorkersCascade()
            hierarchy_nodes                            = getHierarchyList(c)

        ID_library                                     = IDGenerator(c,adp_responses)

        if run_type == 1:
            runType1()
        elif run_type == 2:
            runType2(ID_library)
        elif run_type == 3:
            runType3()
        elif run_type == 4:
            run_type_4()

    countries = ["usa","can"]
    #countries = ["usa"]           #Use to test Country independently)

    run_type = findRunType()
    print (f"Run type {run_type}")

    for c in countries:
        country_choice (c,run_type)

    ct_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ()
    print ("Finished - Putting up my feet (" + ct_fin + ")")
    print ("----------------------------------------------------------------------------------------------------------------")


