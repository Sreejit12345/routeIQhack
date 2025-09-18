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
import pyodbc
import struct

load_dotenv()
app = func.FunctionApp()
vault_url = os.getenv("key_vault_uri")
chunk_size = int(os.getenv("chunk_size", "30"))
connection_str = os.getenv("ODBCConnectionString")

logging.info('Chunk size set to: %d', chunk_size)


@app.blob_trigger(arg_name="myblob", path="ipfiles/in.csv",
                               connection="AzureWebJobsStorage")
@app.sql_output(arg_name="todo",
                        command_text="[routeiq].[FedexTrackingRaw]",
                        connection_string_setting="SqlConnectionString")
 
def routeIQ_file_upload(myblob: func.InputStream,todo: func.Out[func.SqlRow]):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    df = pd.read_csv(myblob)

    delivered_trackingIds = get_all_delivered_trackingIds()

    try:
        df['TRACKINGID'] = df['TRACKINGID'].astype(int)
        delivered_trackingIds_int = [int(x) for x in delivered_trackingIds]
    except Exception as e:
        logging.error(f"Error converting TRACKINGID to int: {e}")
        delivered_trackingIds_int = delivered_trackingIds

    df = df[~df['TRACKINGID'].isin(delivered_trackingIds_int)]

    logging.info(f"Total rows to process after filtering: {len(df)}")

    arr_resp=chunk_rows_and_call_api(df, chunk_size)

    rows = []
    for resp in arr_resp:
        row = func.SqlRow.from_dict({"RawJson": json.dumps(resp), 'TransactionId': resp.get('transactionId', None)})
        rows.append(row)
        
    todo.set(rows)
    
    logging.info("Processing completed successfully.")


def get_all_delivered_trackingIds():

    '''

    Returns a dataframe of all tracking ids which are delivered.

    '''

    delivered_trackingIds=[]

    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            WITH cte AS (
                SELECT
                    t.TransactionId,
                    track.value AS trackingNumber,
                    JSON_VALUE(track.track_results, '$.latestStatusDetail.description') AS description,
                    JSON_VALUE(track.track_results, '$.latestStatusDetail.code') AS latest_status_code,
                    JSON_VALUE(track.track_results, '$.originLocation.locationContactAndAddress.address.city') AS originCity,
                    JSON_VALUE(track.track_results, '$.destinationLocation.locationContactAndAddress.address.city') AS destinationCity,
                    JSON_QUERY(track.track_results, '$.dateAndTimes') AS dateTimeInfo,
                    JSON_QUERY(track.track_results, '$.estimatedDeliveryTimeWindow.window') AS etaWindow
                FROM routeiq.FedexTrackingRaw t
                CROSS APPLY OPENJSON(t.RawJson, '$.output.completeTrackResults')
                    WITH (
                        value NVARCHAR(50) '$.trackingNumber',
                        track_results NVARCHAR(MAX) '$.trackResults[0]' AS JSON
                    ) track
            )
            SELECT DISTINCT trackingNumber FROM cte WHERE description = 'Delivered'
        """)
        for row in cursor.fetchall():
            delivered_trackingIds.append(row[0])
    print(f"Total delivered tracking IDs fetched: {delivered_trackingIds}")
    return delivered_trackingIds
        
    

def get_conn():

    '''

    Connection string for sql server

    '''

    credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect(connection_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn


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
            #['includeDetailedScans']=True

            # call api here

            resp=make_fedex_api_call(bearer_token, "/track/v1/trackingnumbers", payload)
            #print(json.dumps(resp, indent=2))

            outer_array.append(resp)

        c=c+chunk_size

    return outer_array
