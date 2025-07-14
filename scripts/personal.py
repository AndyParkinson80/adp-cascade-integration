from datetime import datetime, timedelta
import os
import json
import requests
import time
import sys
import csv
from io import StringIO

from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import storage

def upload_personal_data_to_cascade(cascade_token,
                                    adp_responses,
                                    adp_terminations,
                                    cascade_responses,
                                    USA,
                                    CAN,
                                    ID_library,
                                    data_store,
                                    Data_export):
    
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating personal details on Cascade (" + time_now + ")")
    x_months_ago = datetime.now() - timedelta(days=180)

    credentials,project = default()
    credentials.refresh(Request())
    storage_client = storage.Client()


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
        
    def convert_adp_to_cascade_form(records,suffix):                         
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
                if USA:
                    display_id = worker["person"]["customFieldGroup"]["stringFields"][2].get("stringValue", "")
                elif CAN:
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

                for entry in ID_library:
                    if entry["ADP_number"] == ADP_id and entry["CascadeId"] is None:
                        contService = entry["contServiceDate"]
                        Id = None
                    elif entry["CascadeId"] == display_id:
                        contService = entry["contServiceDate"]
                        Id = entry["Cascade_full"]
                        break  # Exit the loop once a match is found

                contServiceSplit = contService.split("T")[0]
    
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

    def cascade_rejig():
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

    def combine_json_files():
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

    def POST_new_starters(): 
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
                
            #input("Enter to continue")

    if CAN:
        terminations = load_csv_from_bucket("CAN_termination_mapping")
    elif USA:
        terminations = load_csv_from_bucket("USA_termination_mapping")



    adp_to_cascade                                              = convert_adp_to_cascade_form(adp_responses,"all")
    adp_to_cascade_terminated                                   = convert_adp_to_cascade_form(adp_terminations,"terminated")

    cascade_reordered                                           = cascade_rejig()
    records_to_upload, new_starters, unterminated_staff         = combine_json_files()
    PUT_cascade_workers_personal(records_to_upload)
    PUT_cascade_workers_personal(unterminated_staff)
    POST_new_starters()

if __name__ == "__main__":
    upload_personal_data_to_cascade()