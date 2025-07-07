import os
import requests
import sys
import tempfile

from datetime import datetime

from google.auth import default
from google.cloud import secretmanager
from google.auth.transport.requests import Request

credentials,project = default()
credentials.refresh(Request())

def load_keys(country):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Gathering Security Information (" + time_now + ")")                                                                    #loads keys from secure external file
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Loading Security Keys (" + time_now + ")")
        
        def get_secrets(secret_id):
            def access_secret_version(project_id, secret_id, version_id="latest"):

                client = secretmanager.SecretManagerServiceClient(credentials=credentials)
                name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

                response = client.access_secret_version(request={"name": name})
                payload = response.payload.data.decode("UTF-8")

                return payload

            # Example usage:
            project_id = "api-integrations-412107"
            version_id = "latest" 

            secret = access_secret_version(project_id, secret_id, version_id)
            #print(f"Secret value: {secret}")

            return secret

        client_id = get_secrets(f"ADP-{country}-client-id")
        client_secret = get_secrets(f"ADP-{country}-client-secret")
        country_hierarchy_USA = get_secrets(f"country_Hierarchy_USA")
        country_hierarchy_CAN = get_secrets("country_Hierarchy_CAN")
        strings_to_exclude = get_secrets("strings_to_exclude")
        cascade_API_id = get_secrets("cascade_API_id")
        keyfile = get_secrets(f"{country}_cert_key")
        certfile = get_secrets(f"{country}_cert_pem")
        
        return client_id,client_secret,strings_to_exclude,country_hierarchy_USA,country_hierarchy_CAN,cascade_API_id,keyfile,certfile

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

        # Return the paths of the temporary files
        return temp_certfile.name, temp_keyfile.name
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e

def security(client_id, 
             client_secret, 
             cascade_API_id,
             temp_certfile,
             temp_keyfile):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("        Creating Credentials (" + time_now + ")")
        
        certfile = temp_certfile
        keyfile = temp_keyfile

        def adp_bearer():
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

        def cascade_bearer ():
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
        
        access_token = adp_bearer()
        cascade_token = cascade_bearer()

        return keyfile,certfile,access_token,cascade_token

if __name__ == "__main__":
    country = "usa"

    client_id, \
    client_secret, \
    strings_to_exclude, \
    country_hierarchy_USA, \
    country_hierarchy_CAN, \
    cascade_API_id, \
    keyfile, \
    certfile, = load_keys(country)
    
    temp_certfile, temp_keyfile = load_ssl(certfile, keyfile)


    temp_keyfile, \
    temp_certfile, \
    access_token, \
    cascade_token = security(   client_id, 
                                client_secret, 
                                cascade_API_id,
                                temp_certfile,
                                temp_keyfile)


