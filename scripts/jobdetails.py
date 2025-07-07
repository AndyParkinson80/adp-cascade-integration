from datetime import datetime
import requests
import time
import os
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_UP
import sys


def updating_job_details(
                                                    cascade_token,
                                                    adp_responses,
                                                    Data_export,
                                                    data_store,
                                                    current_folder,
                                                    ID_library,
                                                    USA,CAN):
    

    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating Job details on Cascade (" + time_now + ")")

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

    def cascade_rejig():
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

    def adp_rejig():
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
                if USA:
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

    def adp_rejig_new_starters():
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
                if USA:
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
                            
    def classify_adp_files():
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

    def PUT_update_job_change():
        
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

    def POST_create_jobs():
                
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

    cascade_current_jobs                = cascade_current_workers()
    cascade_current                     = cascade_rejig()
    adp_current,new_starters            = adp_rejig()
    new_start_jobs                      = adp_rejig_new_starters()
    PUT_jobs, POST_jobs                 = classify_adp_files()
    PUT_update_job_change()
    POST_create_jobs()

if __name__ == "__main__":
    updating_job_details()