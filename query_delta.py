import logging
import os
from datetime import datetime
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

spares_storage_account_url="abfss://qmma@wtpsparessapibplake.dfs.core.windows.net/TestFolder/routeiq"
spares_storage_account_name='wtpsparessapibplake'


def read_from_deltalake():
    '''

    Reads the dataframe from delta lake.

    '''

    logging.info("Reading from delta lake...")

    credential = DefaultAzureCredential()
    DEFAULT_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"
    azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)

    try:
        if datetime.fromtimestamp(azure_storage_token.expires_on) <= datetime.now():
            azure_storage_token = credential.get_token(DEFAULT_AZURE_STORAGE_SCOPE)
    except Exception as e:
        logging.error(f"Access token retrieval failed: {e}")
        return None
    
    logging.info("Azure Storage access token obtained successfully.")
    
    # Validate required values
    if not spares_storage_account_name:
        logging.error("spares_storage_account_name is None or empty. Check environment variable 'spares_storage_acc_name'")
        return None
        
    if not spares_storage_account_url:
        logging.error("spares_storage_account_url is None or empty. Check environment variable 'spares_storage_account_url'")
        return None
        
    if not azure_storage_token or not azure_storage_token.token:
        logging.error("Azure storage token is None or empty")
        return None
    
    logging.info(f"Storage account name: {spares_storage_account_name}")
    logging.info(f"Storage account URL: {spares_storage_account_url}")
    
    storage_options = {
        "azure_storage_account_name": spares_storage_account_name,
        "azure_storage_token": azure_storage_token.token,
    }

    try:
        # Create Delta table instance and read data
        delta_table = DeltaTable(spares_storage_account_url, storage_options=storage_options)
        df = delta_table.to_pandas()
        logging.info(f"Successfully read {len(df)} rows from delta table")
        
        # Filter for only the latest timestamp rows
        if len(df) > 0 and 'loadtimestamp' in df.columns:
            # Find the latest timestamp
            latest_timestamp = df['loadtimestamp'].max()
            
            # Filter to only get rows with the latest timestamp
            latest_df = df[df['loadtimestamp'] == latest_timestamp]
            
            logging.info(f"Latest timestamp: {latest_timestamp}")
            logging.info(f"Filtered to {len(latest_df)} rows with latest timestamp")
            
            return latest_df
        else:
            logging.warning("No data found or 'loadtimestamp' column missing")
            return df
            
    except Exception as e:
        logging.error(f"Failed to read from delta table: {e}")
        return None

    logging.info("Finished reading from delta lake...")


df = read_from_deltalake()

if df is not None:
    # Display the latest timestamp data
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    logging.info("Latest timestamp dataframe is \n%s", df)
   
    print(f"Read {len(df)} rows with latest timestamp from Delta table")
    
    # Show timestamp info if available
    if 'loadtimestamp' in df.columns and len(df) > 0:
        latest_timestamp = df['loadtimestamp'].iloc[0]  # All rows should have same timestamp
        print(f"Latest timestamp: {latest_timestamp}")
    
    print("Latest data:")
    print(df)
else:
    # Handle the error case
    print("Failed to read from Delta table")