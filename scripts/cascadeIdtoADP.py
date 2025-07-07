from datetime import datetime
import requests
import math
import time
import os
import json



def push_cascadeId_back_to_ADP(access_token,cascade_token,certfile,keyfile,extended_update,Data_export, data_store,adp_responses,USA,CAN,ID_library):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Pushing Cascade Id's back to ADP (" + time_now + ")")

    def GET_workers_cascade():
            ct_cascade_workers = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print ("        Retrieving updated Personal Data from Cascade HR (" + ct_cascade_workers + ")")

            current_date = datetime.now()
            day_of_week = current_date.weekday()

            cascade_responses = []

            def api_count():
                api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
                api_headers = {
                    'Authorization': f'Bearer {cascade_token}',
                }
                if extended_update == True:                         
                    api_params = {
                        #"$filter": "EmploymentLeftDate eq null",
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
                current_date = datetime.now()
                day_of_week = current_date.weekday()
                
                if day_of_week == 1:                         
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

            if Data_export:
                file_path = os.path.join(data_store,"006 - CascadeId to ADP","001 - Cascade (Data Out).json")
                with open(file_path, "w") as outfile:
                    json.dump(combined_data, outfile, indent=4)

            return combined_data

    def whats_in_ADP():
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
                if USA:
                    Cascade = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
                if CAN:
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
                        }

                    ID_responses.append(transformed_record)
        
        if Data_export:
            file_path = os.path.join(data_store,"006 - CascadeId to ADP","003 - IDs_updating.json")
            with open(file_path, "w") as outfile:
                json.dump(ID_responses, outfile, indent=4)
        
        return ID_responses

    def upload_cascade_Ids_to_ADP():
        ct_POST_cascade_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Updating Cascade ID's on WFN (" + ct_POST_cascade_id + ")")

        api_url = 'https://api.adp.com/events/hr/v1/worker.person.custom-field.string.change'
        
        for entry in CascadeId_to_upload:
            AOID = entry['AOID']
            if USA:
                ItemID = '9200019333951_24129'         
            if CAN:
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
    
    cascade_responses_II            = GET_workers_cascade()
    CascadeId_to_upload             = whats_in_ADP()
    upload_cascade_Ids_to_ADP()

    return cascade_responses_II

if __name__ == "__main__":
    push_cascadeId_back_to_ADP()

print()