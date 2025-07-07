import os
import pandas as pd
import json
from datetime import datetime, timedelta
import requests
import time
import sys
from pathlib import Path
from google.cloud import bigquery


def updating_absences_from_cascade(             access_token,
                                                cascade_token,
                                                certfile,
                                                keyfile,
                                                Data_export,
                                                data_store,
                                                USA,
                                                CAN,
                                                current_folder,
                                                country,
                                                ID_library,
                                                run_type):
    
    data_store = Path(data_store)
    
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating Absence details on Cascade (" + time_now + ")")

    file_path = os.path.join(current_folder,"Hierarchy.xlsx")                                               #creates a dictionary to link ADP and cascade absence codes
    df = pd.read_excel(file_path,sheet_name=f"{country} Absences")

    if USA:                                                                                                 #pulls the conversion table from Hierarchy.xlsx
        absence_reasons = df.where(pd.notna(df), None).to_dict(orient='records')
        for item in absence_reasons:
            item["policy"] = str(item["policy"])
            item["earningType"] = str(item["earningType"])
            item["narrative"] = str(item["narrative"])
            item["cascadeAbsenceId"] = str(item["cascadeAbsenceId"])
    elif CAN:
        absence_reasons = df.where(pd.notna(df), None).to_dict(orient='records')
        for item in absence_reasons:
            item["Name"] = str(item["Name"])
            item["Id"] = str(item["Id"])
    
    if Data_export:
        file_path = data_store/"005 - Absences to Cascade"/f"001 - {country} absence reasons.json"
        print (file_path)
        with open(file_path, "w") as outfile:
            json.dump(absence_reasons, outfile, indent=4)
    
    ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
    absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    def get_cascade_id(CascadeId):                                                                          #Find the Unique Id for both ADP and Cascade
        for record in ID_library:
            if record["CascadeId"] == CascadeId:
                Cascade_full = record["Cascade_full"]
                AOID = record["AOID"]
                break
        
        return Cascade_full,AOID
   
    def get_absences_adp(AOID):                                                                                                                                                 #For the current employee, finds their absences on ADP

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

    def convert_ADP_absences_to_cascade_format(adp_response):
        def calculate_duration_minutes(total_quantity):
            """Calculate duration minutes based on total quantity."""
            if total_quantity['unitTimeCode'].lower() == 'hour':
                return int(total_quantity['valueNumber'] * 60)
            elif 'min' in total_quantity['unitTimeCode'].lower():
                return int(total_quantity['valueNumber'])
            return 0

        def determine_day_part(duration_minutes, start_time):
            """Determine day part based on duration and start time."""
            if duration_minutes > 360:
                return "AllDay"
            
            # Convert start time to hour
            start_hour = int(start_time.split(':')[0])
            
            if duration_minutes < 360:
                return "AM" if start_hour < 12 else "PM"
            
            return "AllDay"

        def resolve_absence_reason_id(main_record, absence_reasons):
            """
            Resolve the AbsenceReasonId based on policy and earning type matching.
            
            Args:
                main_record (dict): The main record containing paid time off entries
                absence_reasons (list): List of available absence reasons
            
            Returns:
                int or None: Matched AbsenceReasonId or None if no match found
            """
            try:
                # Extract policy and earning type from the first paid time off entry
                first_entry = main_record['paidTimeOffEntries'][0]
                policy_label = first_entry['paidTimeOffPolicy']['labelName']
                earning_type_label = first_entry['earningType']['labelName']
                
                # Find matching absence reason
                matching_absence = next(
                    (absence['cascadeAbsenceId'] 
                    for absence in absence_reasons 
                    if (absence['policy'] == policy_label and 
                        absence['earningType'] == earning_type_label)), 
                    None
                )
                
                return matching_absence
            
            except (KeyError, IndexError, StopIteration) as e:
                # Log the error or handle it appropriately
                print(f"Error resolving AbsenceReasonId: {e}")
                return None

        output = {"Pending": [], "Approved": [], "Cancelled": []}

        for category_index, category in enumerate(adp_response['paidTimeOffDetails']['paidTimeOffRequests'][0]['paidTimeOffRequestEntries']):
            absence_type = category['requestStatus']['labelName']

            for record_index, main_record in enumerate(category['requests']):
                new_record = {f"Record Position {record_index}": {
                    "EmployeeId": Cascade_full,
                    "AbsenceReasonId": resolve_absence_reason_id(main_record, absence_reasons),

                    "Label name": main_record['paidTimeOffEntries'][0]['earningType']['labelName'],
                    "Narrative": None,
                    "StartDate": main_record['paidTimeOffEntries'][0]['timePeriod']['startDateTime'],
                    "EndDate": main_record['paidTimeOffEntries'][-1]['timePeriod']['endDateTime'],
                    "DayInfo": []
                }}

                # Process DayInfo for each paid time off entry
                for entry in main_record['paidTimeOffEntries']:
                    duration_minutes = calculate_duration_minutes(entry['totalQuantity'])
                    day_part = determine_day_part(duration_minutes, entry.get('startTime', '08:00'))

                    day_info = {
                        "Date": entry['timePeriod']['startDateTime'],
                        "DurationDays": 1,
                        "DurationMinutes": duration_minutes,
                        "DayPart": day_part
                    }
                    new_record[f"Record Position {record_index}"]["DayInfo"].append(day_info)

                # Categorize based on absence type
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

        transformed_approved_absences = [
            record[f"Record Position {i}"] 
            for i, record in enumerate(approved_absences)
        ]

        all_absences.extend(transformed_approved_absences)


        if Data_export:
            file_path = os.path.join(data_store, "005 - Absences to Cascade", "003 - ADP absences - categorised.json")
            with open(file_path, "w") as json_file:
                json.dump(final_output, json_file, indent=4)
            file_path = os.path.join(data_store, "005 - Absences to Cascade", "003a - ADP absences - Approved.json")
            with open(file_path, "w") as json_file:
                json.dump(transformed_approved_absences, json_file, indent=4)


        return transformed_approved_absences

    def check_absences():
        print ("Checking Queries against bigQuery absences")
        client = bigquery.Client()

        query = """
        SELECT EmployeeId, AbsenceReasonId, StartDate, EndDate
        FROM `api-integrations-412107.absences_database.absences`
        """

        query_job = client.query(query)
        existing_absences = {  # Store in a set for faster lookup
            (row.EmployeeId, row.AbsenceReasonId, row.StartDate, row.EndDate)
            for row in query_job
        }

        unmatched_absences = []

        for absence in all_absences:
            key = (
                absence["EmployeeId"],
                absence["AbsenceReasonId"],
                absence["StartDate"],
                absence["EndDate"]
            )
            
            if key not in existing_absences:
                unmatched_absences.append(absence)
            
            if Data_export:
                file_path = os.path.join(data_store,"005 - Absences to Cascade","005 - Unmatched Absences.json")
                with open(file_path, "w") as outfile:
                    json.dump(unmatched_absences, outfile, indent=4)


    all_absences = []

    for record in ID_library:
        CascadeId = record["CascadeId"]
        print(f"Updating absences for {CascadeId}")
        Cascade_full, AOID = get_cascade_id(CascadeId)
       
        
        try:
            adp_response = get_absences_adp(AOID)
            adp_current = convert_ADP_absences_to_cascade_format(adp_response)
        except Exception as e:
            print(f"Error processing CascadeId {CascadeId}: {e}")
            continue

        if Data_export:
            file_path = os.path.join(data_store,"005 - Absences to Cascade","004 - All Absences.json")
            with open(file_path, "w") as outfile:
                json.dump(all_absences, outfile, indent=4)
        
    check_absences()


if __name__ == "__main__":
    updating_absences_from_cascade()