import os
import pandas as pd
import json
from datetime import datetime, timedelta
import requests
import time
import sys
from google.cloud import storage


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
    
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Updating Absence details on Cascade (" + time_now + ")")

    file_path = os.path.join(current_folder,"Hierarchy.xlsx")                                               #creates a dictionary to link ADP and cascade absence codes
    df = pd.read_excel(file_path,sheet_name=f"{country} Absences")

    if USA:
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
        file_path = os.path.join(data_store,"005 - Absences to Cascade",f"001 - {country} absence reasons.json")
        with open(file_path, "w") as outfile:
            json.dump(absence_reasons, outfile, indent=4)
    
    ninety_days_ago = datetime.now() - timedelta(days=90)                                                   # ADP only returns last 90, this allows the same for cascade
    absences_from = ninety_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    def get_cascade_id(CascadeId):
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

    def convert_ADP_absences_to_cascade_format():
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
                if USA:
                    absenceEarningType = main["paidTimeOffEntries"][0]["earningType"].get("labelName", "")
                    for absence in absence_reasons:
                        if absence['policy'] == absencePolicy and absence['earningType'] == absenceEarningType:
                            AbsenceReasonId = absence['cascadeAbsenceId']
                if CAN:
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
                        "Label name": absenceEarningType if USA else None,
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

    def cascade_absences():

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

    def combine_json_files_for_POST():
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

    def POST(new_records):
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
                if USA:
                    hours = address["totalQuantity"]["valueNumber"]
                    days = hours/8
                    minutes = hours * 60
                elif CAN:
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

    all_absences = []

    #Create a subset of the ID Library from the absences identified in the event download.

    def load_from_bucket(variable):
        client = storage.Client()
        bucket = client.bucket("event_list_objects")
        blob = bucket.blob(f"{variable}.json")

        data = json.loads(blob.download_as_text())
        string_list = data["strings"]

        return string_list

    if USA:
        absences = load_from_bucket("Absences")
        cancellations = load_from_bucket("Cancellations")
        ID_list = [record for record in ID_library if record["AOID"] in absences or record["AOID"] in cancellations]
    
    else:
        ID_list = ID_library

    for record in ID_list:
        CascadeId = record["CascadeId"]
        print(f"Updating absences for {CascadeId}")
        Cascade_full, AOID = get_cascade_id(CascadeId)            
        
        try:
            adp_response = get_absences_adp(AOID)                               #Downloads the absences in the last 90 days for a given staff member
            adp_current = convert_ADP_absences_to_cascade_format()              #Converts ADP absences into Cascade format

            if len(adp_current) == 0:
                print(f"        No booked absences for {CascadeId}")
                continue  # If there are no absences, skip to the next record
            else:
                cascade_current, current_absence_id_cascade = cascade_absences()  # Pulls list of current absences
                new_records, Update_transformed, delete_ids, update_ids = combine_json_files_for_POST()  # Compares adp and cascade and removes any that are already in cascade

            DELETE(delete_ids)  # Deletes cancelled absences'''
            if run_type ==5:
                POST(new_records)  # Creates new absences

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

                                                                             #Just updates cascadeIds (00:00)
    
if __name__ == "__main__":
    updating_absences_from_cascade()