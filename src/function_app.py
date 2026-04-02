from fedex_creator import FEDEXAPICreator
from dhl_creator import DHLAPICreator
import logging
import azure.functions as func
import pandas as pd
import os
import uuid
import pyodbc
import struct
from datetime import datetime
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable, write_deltalake

load_dotenv()
app = func.FunctionApp()
vault_url = os.getenv("key_vault_uri")
connection_str = os.getenv("ODBCConnectionString")
spares_storage_account_url = os.getenv("spares_storage_account_url")
spares_storage_account_name = os.getenv("spares_storage_acc_name")


@app.blob_trigger(arg_name="myblob", path="ipfiles/in.csv",
                               connection="AzureWebJobsStorage") 
def routeIQ_file_upload(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    df = pd.read_csv(myblob,keep_default_na=False)
    df = df.drop_duplicates(subset=['TRACKINGID','carrier_type'])

    # convert to strategy pattern later
    
    df_fedex = df[df['carrier_type'].str.lower() == 'fedex']
    df_dhl = df[df['carrier_type'].str.lower() == 'dhl']

    fedex_obj=FEDEXAPICreator().factory_method()
    dhl_obj=DHLAPICreator().factory_method()

    result_fedex=fedex_obj.curate_api_response(df_fedex)
    result_dhl=dhl_obj.curate_api_response(df_dhl)

    combined_df_fedex=fedex_obj.generate_final_output(result_fedex)
    combined_df_dhl=dhl_obj.generate_final_output(result_dhl)
   
    write_to_deltalake(combined_df_fedex)
    write_to_deltalake(combined_df_dhl)
    
    logging.info("Processing/file write completed successfully.")


def get_all_delivered_trackingIds():

    '''

    Returns a list of all tracking ids which are delivered.

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

def get_all_error_trackingIds():

    '''

    Returns a list of all tracking ids which are errored

    '''

    errored_trackingIds=[]

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
                    JSON_QUERY(track.track_results, '$.estimatedDeliveryTimeWindow.window') AS etaWindow,
                    JSON_VALUE(track.track_results, '$.error.code') AS error_code
                FROM routeiq.FedexTrackingRaw t
                CROSS APPLY OPENJSON(t.RawJson, '$.output.completeTrackResults')
                    WITH (
                        value NVARCHAR(50) '$.trackingNumber',
                        track_results NVARCHAR(MAX) '$.trackResults[0]' AS JSON
                    ) track
            )
            SELECT distinct trackingNumber
            FROM cte
            where error_code is not null
        """)
        for row in cursor.fetchall():
            errored_trackingIds.append(row[0])
    print(f"Total errored tracking IDs fetched: {errored_trackingIds}")
    return errored_trackingIds
        
    

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


def write_to_deltalake(df):
    '''

    Writes the dataframe to delta lake.

    '''

    logging.info("Writing to delta lake...")

    credential = DefaultAzureCredential()
    DEFAULT_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"
    azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)

    try:
        if datetime.fromtimestamp(azure_storage_token.expires_on) <= datetime.now():
            azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)
    except Exception as e:
        logging.error(f"Access token retrieval failed: {e}")
    
    logging.info("Azure Storage access token obtained successfully.")
    
    storage_options = {
    "azure_storage_account_name": spares_storage_account_name,
    "azure_storage_token": azure_storage_token.token,
    }


    try:
        _ = DeltaTable(spares_storage_account_url, storage_options=storage_options)
        table_exists = True
        logging.info("Delta table exists, will append.")
    except Exception as e:
        table_exists = False
        logging.info(f"Delta table does not exist or cannot access, will create new. Error: {e}")

    write_deltalake(
    spares_storage_account_url,
    df,
    mode="append" if table_exists else "overwrite",
    storage_options=storage_options,
    schema_mode='merge'
    )

    logging.info("Finished writing to delta lake...")

    