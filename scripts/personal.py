from datetime import datetime
import os
import json
import requests
import time

def upload_personal_data_to_cascade(cascade_token,
                                    adp_responses,
                                    cascade_responses,
                                    USA,
                                    CAN,
                                    ID_library,
                                    data_store,
                                    Data_export):
    
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating personal details on Cascade (" + time_now + ")")
        
    def convert_adp_to_cascade_form():                         
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Converting the adp data to the cascade form (" + time_now+ ")")

        # Create a dictionary to generate IRIS Id from CascadeId
        output = []  

        
        # Iterate through each record and extract the required fields
        for record in adp_responses:
            workers = record["workers"]
            for worker in workers:
                active_job_position = None

                work_assignments = worker.get("workAssignments", [{}])
                for index, assignment in enumerate(work_assignments):
                    if assignment.get("primaryIndicator", True):
                        active_job_position = index
                        continue
            
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

                #Change any of the language from ADP to Iris HR        
                if WorkingStatus == "Active":
                    WorkingStatus = "Current"
                if WorkingStatus == "Inactive":                     #This may be removed later - discussion needed AP/KG
                    WorkingStatus = "Current"
                if mobileOwner == "Personal Cell":
                    mobileOwner = "Personal"
                if WorkingStatus == "Current":                      #Override for previous termination date
                    end_date = None
                if mobileOwner == None:
                    mobileOwner = "Personal"

                #converts the dats strings into a datetime format for comparison

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
                    "Gender": None,
                    "Ethnicity": None,
                    "Nationality": None,
                    "Religion": None,
                    "LeaverReason": None,
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

                output.append(transformed_record)
                
                if Data_export:
                    file_path = os.path.join(data_store,"003 - Personal to Cascade","001 - ADP_to_cascade.json")
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
        
        return update_personal,new_starters

    def PUT_cascade_workers_personal():
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Updating Staff changes (" + time_now + ")")                         
        
        for entry in records_to_upload:
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
                print("             " + f'Personal information transfer for {employee_id} ({display_id}) complete. {response.status_code}')
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

    adp_to_cascade                                              = convert_adp_to_cascade_form()
    cascade_reordered                                           = cascade_rejig()
    records_to_upload, new_starters                             = combine_json_files()
    PUT_cascade_workers_personal()
    POST_new_starters()

if __name__ == "__main__":
    upload_personal_data_to_cascade()