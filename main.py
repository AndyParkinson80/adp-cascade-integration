# Set-ExecutionPolicy Bypass -Scope Process
# cd "C:\\Users\\andre\OneDrive - acornstairlifts.com\\001 - Data + Systems\\000 - Deployed Programs\\001 - ADP to Cascade Integration (USA and CAN)"
# gcloud builds submit --tag europe-west2-docker.pkg.dev/api-integrations-412107/integration/integration:latest
# gcloud run jobs update adp-integrations --image europe-west2-docker.pkg.dev/api-integrations-412107/integration/integration:latest --region europe-west2
# gcloud run jobs execute adp-integrations --region europe-west2

from datetime import datetime
import os
import sys
from pathlib import Path

from scripts import securityKeys, globalcalls, cascadeIdtoADP, personal
from scripts import jobdetails, absences, folder_structure, run_type_choice

debug = True

if debug is True:
    folder_structure.create_folders()
else:
    folder_structure.delete_folders()

run_type = run_type_choice.find_run_type()
run_type = 2

def country(country,USA,CAN,run_type,debug):
    print ("---------------------------------------------------------------------------------------------------------------")
    print (f"Synchronizing country: {country}")
    
    if debug:
        extended_update = False                                          
        Data_export = True
    else:
        extended_update = True                                                            
        Data_export = False      

    current_folder = Path(__file__).resolve().parent
    if USA:                                                                             
        data_store = os.path.join(current_folder,"Data Store","Data - USA")
    elif CAN:
        data_store = os.path.join(current_folder,"Data Store","Data - CAN")


    client_id, \
    client_secret, \
    strings_to_exclude, \
    country_hierarchy_USA, \
    country_hierarchy_CAN, \
    cascade_API_id, \
    keyfile, \
    certfile \
        = securityKeys.load_keys(                   country)
    
    temp_certfile, temp_keyfile = securityKeys.load_ssl(certfile, keyfile)
    
    keyfile, \
    certfile, \
    access_token, \
    cascade_token  \
        = securityKeys.security(                    client_id, 
                                                    client_secret, 
                                                    cascade_API_id,
                                                    temp_certfile,
                                                    temp_keyfile)
    
    adp_responses, \
    cascade_responses, \
    hierarchy_nodes, \
    ID_library  \
        = globalcalls.global_data_calls(            access_token,
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
                                                    country)

    if run_type == 1:
        cascadeIdtoADP.push_cascadeId_back_to_ADP(  access_token,
                                                    cascade_token,
                                                    certfile,
                                                    keyfile,
                                                    extended_update,
                                                    Data_export, 
                                                    data_store,
                                                    adp_responses,
                                                    USA,
                                                    CAN,
                                                    ID_library)
        
    elif run_type == 2:
        absences.updating_absences_from_cascade(    access_token,
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
                                                    run_type)  
    
    elif run_type == 3:
        personal.upload_personal_data_to_cascade(   cascade_token,
                                                    adp_responses,
                                                    cascade_responses,
                                                    USA,
                                                    CAN,
                                                    ID_library,
                                                    data_store,
                                                    Data_export)
    
    elif run_type == 4:
        jobdetails.updating_job_details(            cascade_token,
                                                    adp_responses,
                                                    Data_export,
                                                    data_store,
                                                    current_folder,
                                                    ID_library,
                                                    USA,CAN)

    elif run_type == 5:
        absences.updating_absences_from_cascade(    access_token,
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
                                                    run_type)

    else:
        print("Run Type not defined correctly. Set Flag in line 15")       

country ("usa",True,False,run_type,debug)
country ("can",False,True,run_type,debug)

ct_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print ()
print ("Finished - Putting up my feet (" + ct_fin + ")")
print ("---------------------------------------------------------------------------------------------------------------")


