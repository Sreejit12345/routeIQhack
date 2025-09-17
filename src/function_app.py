import azure.functions as func
import datetime
import json
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import os
import requests
import pandas as pd

load_dotenv()
app = func.FunctionApp()
vault_url = os.getenv("key_vault_uri")
chunk_size = int(os.getenv("chunk_size", "3"))


@app.blob_trigger(arg_name="myblob", path="ipfiles/in.csv",
                               connection="AzureWebJobsStorage") 
def routeIQ_file_upload(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    df = pd.read_csv(myblob)

    chunk_rows_and_create_payload(df, 3)


def get_secret_from_key_vault(vault_url, secret_name):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def get_bearer_token(client_id, client_secret):


    url = "https://apis.fedex.com/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    token_info = response.json()
    return token_info["access_token"]

def make_fedex_api_call(bearer_token, endpoint, payload):
    url = f"https://apis.fedex.com{endpoint}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def chunk_rows_and_call_api(df, chunk_size):

    '''

    Chunks rows into chunks of specified size and creates payload and calls FedEx API for each chunk.

    '''

    row_count = df['TRACKINGID'].count()
    
    outer_array=[]

    c=0

    bearer_token= get_bearer_token(get_secret_from_key_vault(vault_url, "fedexclientid"),get_secret_from_key_vault(vault_url, "fedexclientsecret"))

    while(c<=row_count):

        inner_array=[]

        chunk_count=0

        payload={}

        for index,rows in df.iloc[c:chunk_size+c].iterrows():

            if(chunk_count==chunk_size):
                break

            inner_array.append({'trackingNumberInfo':{'trackingNumber': rows['TRACKINGID']}})
            chunk_count+=1



        if(len(inner_array)>0):
            payload['trackingInfo']=inner_array
            payload['includeDetailedScans']=True

            # call api here

            resp=make_fedex_api_call(bearer_token, "/track/v1/trackingnumbers", payload)


            outer_array.append(inner_array)

        c=c+chunk_size
