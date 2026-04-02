import requests
import logging  
import time
import json
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import pandas as pd
import uuid
from datetime import datetime
from api_factory import APIFactory


class FEDEXAPI(APIFactory):

    token_url = "https://apis.fedex.com/oauth/token"
    response_url = "https://apis.fedex.com/track/v1/trackingnumbers"
    keyvault_url = "https://kv-routeiq.vault.azure.net/"
    secret_client_id_name = "FedExClientId"
    secret_client_secret_name = "FedExClientSecret"
    chunk_size = 30

    def fetch_secret(self,vault_url, secret_name):
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        secret = client.get_secret(secret_name)
        return secret.value

    def generate_bearer_token(self):

        client_id=self.fetch_secret(FEDEXAPI.keyvault_url, FEDEXAPI.secret_client_id_name)
        client_secret=self.fetch_secret(FEDEXAPI.keyvault_url, FEDEXAPI.secret_client_secret_name)

        headers = {
        "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
        }

        response = requests.post(FEDEXAPI.token_url, headers=headers, data=data)

        response.raise_for_status()
        token_info = response.json()

        logging.info("Bearer token obtained successfully.")

        return token_info["access_token"]


    def call_api(self,payload=None):

        bt=self.generate_bearer_token()

        url = f"{FEDEXAPI.response_url}"
        headers = {
            "Authorization": f"Bearer {bt}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()



    def curate_api_response(self,df):

        '''
        Chunks rows into chunks of specified size and creates payload and calls FedEx API for each chunk.
        '''
        
        row_count = len(df)
        
        outer_array=[]

        c=0

        while(c < row_count):
            

            inner_array=[]

            chunk_count=0

            payload={}

            chunk_end = min(c + FEDEXAPI.chunk_size, row_count)
            
            for index,rows in df.iloc[c:chunk_end].iterrows():

                inner_array.append({'trackingNumberInfo':{'trackingNumber': rows['TRACKINGID']}})
                chunk_count+=1


            if(len(inner_array)>0):
                payload['trackingInfo']=inner_array
                #['includeDetailedScans']=True
                #logging.info("payload is {payload}".format(payload=json.dumps(payload)))
                
                retry_limit=5
                retry_count = 0

                while(retry_count < retry_limit):

                    if(retry_count > 0):
                        logging.info(f"Retrying API call for chunk starting at row {c}. Attempt {retry_count} of {retry_limit}.")
                        time.sleep(300) # wait for 5 minutes before retrying

                    try:
                        has_exception = False
                        #retry_count=retry_count+1
                        resp=self.call_api(payload)

                        outer_array.append(resp)

                    except Exception as e:
                        has_exception = True
                        retry_count += 1
                        logging.error(f"API call failed for chunk starting at row {c} with error: {e}")
                    
                    if not has_exception:
                        break

            
            logging.info(f"Chunk from row {c} to {chunk_end} processed.")
            time.sleep(1)

            c=c+FEDEXAPI.chunk_size

        outer_array=self.append_identifier_to_response(outer_array, df)
        
        return outer_array

        
    def append_identifier_to_response(self,outer_array, df):

        # Check if required columns exist, return early if not
        if 'NotificationId' not in df.columns:
            logging.info("NotificationId column missing, skipping identifier mapping")
            return outer_array
        
        id_map = {
            str(row['TRACKINGID']): {
                'NotificationId':row['NotificationId']
            }
            for _, row in df.iterrows()
        }

        for resp in outer_array:
            for result in resp.get('output', {}).get('completeTrackResults', []):
                tracking_id = str(result.get('trackingNumber'))
                if tracking_id in id_map:
                    result['NotificationId'] = id_map[tracking_id]['NotificationId']

        return outer_array



    def parse_tracking_fields(self, track_result):
        """
        Parse specific fields from FedEx tracking API response
        """
        parsed_data = {
            'trackingNumber': track_result.get('trackingNumber', 'N/A'),
            'NotificationId': track_result.get('NotificationId', 'N/A'),
            'actualPickup': 'N/A',
            'actualDelivery': 'N/A', 
            'receivedByName': 'N/A',
            'latestStatus': 'N/A',
            'latestStatusCode': 'N/A',
            'isErrored': False,
            'errorDescription': 'N/A'
        }
        
        # Get the main track results
        track_results = track_result.get('trackResults', [])
        if track_results:
            main_result = track_results[0]

            #error
            if main_result.get('error') is not None:
                parsed_data['isErrored'] = True
                parsed_data['errorDescription'] = main_result.get('error', {}).get('message', 'N/A')
            
            # status detail
            latest_status = main_result.get('latestStatusDetail', {})
            if latest_status:
                parsed_data['latestStatus'] = latest_status.get('description', 'N/A')
                parsed_data['latestStatusCode'] = latest_status.get('code', 'N/A')
            
            # delivery details
            delivery_details = main_result.get('deliveryDetails', {})
            if delivery_details:
                parsed_data['receivedByName'] = delivery_details.get('receivedByName', 'N/A')
                
            #  pickup details and delivery details
            date_times = main_result.get('dateAndTimes', [])
            for date_time in date_times:
                if date_time.get('type') == 'ACTUAL_PICKUP':
                    parsed_data['actualPickup'] = date_time.get('dateTime', 'N/A')
                elif date_time.get('type') == 'ACTUAL_DELIVERY':
                    parsed_data['actualDelivery'] = date_time.get('dateTime', 'N/A')
                    

        return parsed_data


    def generate_final_output(self,result):

        schema={
        'transactionId': 'string',
        'trackingNumber': 'string',
        'NotificationId': 'string',
        'actualPickup': 'string',
        'actualDelivery': 'string',
        'receivedByName': 'string',
        'latestStatus': 'string',
        'latestStatusCode': 'string',
        'loadtimestamp': 'string',
        'isErrored': 'boolean',
        'errorDescription': 'string'
    }

        combined_df = pd.DataFrame(columns=['transactionId', 'trackingNumber', 'NotificationId', 'actualPickup', 'actualDelivery', 
                                        'receivedByName', 'latestStatus', 'latestStatusCode', 'loadtimestamp', 'isErrored', 'errorDescription']).astype(schema)
    
        for i, response in enumerate(result):

            transaction_id=response.get('transactionId', f'{uuid.uuid4()}')

            for track_result in response.get('output', {}).get('completeTrackResults', []):
                parsed_data = self.parse_tracking_fields(track_result)
                parsed_data['transactionId'] = transaction_id

        
                temp_df = pd.DataFrame([parsed_data], dtype='string')
                combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

        # Add load timestamp to all rows
        combined_df['loadtimestamp'] = datetime.now().isoformat()

        return combined_df