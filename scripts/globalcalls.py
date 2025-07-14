from datetime import datetime
import requests
import math
import time
import json
import os
import sys
import pandas as pd

def global_data_calls(access_token,
                      cascade_token,
                      certfile,
                      keyfile,
                      strings_to_exclude,
                      extended_update,
                      Data_export,
                      data_store,
                      country_hierarchy_USA,
                      country_hierarchy_CAN,
                      USA,
                      CAN,
                      current_folder,
                      country):
    
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Making global calls (" + time_now + ")")

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

        # Final separate combined structures
        combined_data_responses = [{
            "workers": combined_workers_responses,
            "meta": None,
            "confirmMessage": None
        }]

        combined_data_terminated = [{
            "workers": combined_workers_terminated,
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

        cascade_responses = []

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
                    #"$filter": "EmploymentLeftDate eq null",
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
                cascade_responses.append(json_data)

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
        for item in cascade_responses:
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
            file_path = os.path.join(data_store,"002 - Security and Global","001 - Cascade (Data Out).json")
            with open(file_path, "w") as outfile:
                json.dump(filtered_data, outfile, indent=4)

        return filtered_data  

    def GET_hierarchy_nodes():
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Retrieving Job Hierarchy Nodes (" + time_now + ")")

        if USA:
            H_top_level = country_hierarchy_USA
        elif CAN:
            H_top_level = country_hierarchy_CAN
        
        api_url = 'https://api.iris.co.uk/hr/v2/hierarchy'
        api_headers =   {
            'Authorization': f'Bearer {cascade_token}',
                        }
        
        def get_hierarchy_nodes(hierarchy_ids):
            hierarchy_nodes = []
            hierarchy_id_nodes = []

            for h_id in hierarchy_ids:
                api_params = {
                    "$filter": f"parentId eq '{h_id}' and disabled eq false"
                }

                response = requests.get(api_url, params=api_params, headers=api_headers)
                time.sleep(0.6)
                if response.status_code == 200:
                    data = response.json()
                    for record in data.get('value', []):
                        hierarchy_nodes.append(record)
                        hierarchy_id_nodes.append(record['Id'])
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

    def ID_generator():
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
                    if USA:
                        job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][1]["nameCode"]["codeValue"]
                        file_path = os.path.join(current_folder,"Hierarchy.xlsx")
                        df = pd.read_excel(file_path,sheet_name=f"{country} Conversion")
                    
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
                            #for record in entry["value"]:
                            if entry["NationalInsuranceNumber"] == ADP_identifier:
                                CascadeID = entry["DisplayId"]
                                if CascadeID is None:
                                    Cascade_full = None
                                else:    
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
                            "Hierarchy": hierarchy_id,
                            "Cascade Start": contServiceCascade,
                            "ADP Start": formatted_date,
                            "contServiceDate": date,
                        }
                        ID_library.append(transformed_record)

                    elif CAN:
                        job_code = worker["workAssignments"][active_job_position]["homeOrganizationalUnits"][0]["nameCode"]["codeValue"]
                        job_name = worker["workAssignments"][active_job_position]["jobTitle"]
                        file_path = os.path.join(current_folder,"Hierarchy.xlsx")
                        df = pd.read_excel(file_path,sheet_name=f"{country} Conversion")
                        
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

    adp_responses, adp_terminations                                                                         = GET_workers_adp()
    cascade_responses                                                                                       = GET_workers_cascade()
    hierarchy_nodes                                                                                         = GET_hierarchy_nodes()
    ID_library                                                                                              = ID_generator()

    return (adp_responses,adp_terminations,cascade_responses,hierarchy_nodes,ID_library)

if __name__ == "__main__":
    adp_responses,adp_terminations,cascade_responses,hierarchy_nodes,ID_library = global_data_calls()

print()