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


class DHLAPI(APIFactory):

    response_url = "https://api-eu.dhl.com/track/shipments"
    keyvault_url = "https://kv-routeiq.vault.azure.net/"
    secret_name = "DHLClientSecret"

    def fetch_secret(self,vault_url, secret_name):
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        secret = client.get_secret(secret_name)
        return secret.value

    def generate_bearer_token(self):
        
        """
        DHL uses API Keys instead of bearer tokens
        This method is not used for DHL API
        """
        return None


    def call_api(self,payload=None):

        dhl_secret=self.fetch_secret(DHLAPI.keyvault_url, DHLAPI.secret_name)
        
        tracking_number = payload.get("trackingNumber")

        url = f"{DHLAPI.response_url}?trackingNumber={tracking_number}"

        headers = {
            "DHL-API-Key": f"{dhl_secret}",

        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        d=response.json()
        d['trackingId']=tracking_number
        return d

    def curate_api_response(self,df):
        
        outer_array=[]

        for index,rows in df.iterrows():
            
            payload = {
            "trackingNumber": str(rows['TRACKINGID'])}

            retry_limit=2
            retry_count = 0

            while(retry_count < retry_limit):

                if(retry_count > 0):
                    logging.info(f"Retrying API call for tracking number {rows['TRACKINGID']}. Attempt {retry_count} of {retry_limit}.")
                    time.sleep(30) # wait for retry delay

                try:
                    has_exception = False
                    resp=self.call_api(payload)
                    outer_array.append(resp)

                except Exception as e:
                    has_exception = True
                    logging.error(f"API call failed for tracking number {rows['TRACKINGID']} with error: {e}")
                
                retry_count += 1
                
                if has_exception ==False:
                    break

                time.sleep(30) # wait for retry delay
            
            if(has_exception == True):
                outer_array.append({'trackingId': str(rows['TRACKINGID'])})  #append empty response- so it gets marked as error later on

        
        outer_array = self.append_identifier_to_response(outer_array, df)

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
            tracking_id = str(resp.get('trackingId'))
            if tracking_id in id_map:
                resp['NotificationId'] = id_map[tracking_id]['NotificationId']

        return outer_array



    def parse_tracking_fields(self, track_result):
        """
        Parse specific fields from DHL tracking API response based on OpenAPI specification
        """
        parsed_data = {
            'trackingNumber': track_result.get('trackingId', 'N/A'),
            'NotificationId': track_result.get('NotificationId', 'N/A'),
            'actualPickup': 'N/A',
            'actualDelivery': 'N/A', 
            'receivedByName': 'N/A',
            'latestStatus': 'N/A',
            'latestStatusCode': 'N/A',
            'isErrored': False,
            'errorDescription': 'N/A'
        }
        
        # if shipment data is missing, mark as errored
        shipments = track_result.get('shipments', [])
        if not shipments:
            parsed_data['isErrored'] = True
            parsed_data['errorDescription'] = 'No shipment data found'
            return parsed_data
        
        # Get the first shipment
        shipment = shipments[0]
        
        # Get latest status from shipment status
        status_info = shipment.get('status', {})
        if status_info:
            parsed_data['latestStatus'] = status_info.get('status', 'N/A')
            parsed_data['latestStatusCode'] = status_info.get('statusCode', 'N/A')
            
            # Check if delivered (per API spec: delivered, failure, pre-transit, transit, unknown)
            if status_info.get('statusCode', '').lower() == 'delivered':
                parsed_data['actualDelivery'] = status_info.get('timestamp', 'N/A')
        
        # Get pickup date from shipment level field (per API spec)
        pickup_date = shipment.get('pickUpDate')
        if pickup_date and pickup_date != 'N/A':
            parsed_data['actualPickup'] = pickup_date
        
        # Check for proof of delivery information
        details = shipment.get('details', {})
        if details:
            proof_of_delivery = details.get('proofOfDelivery', {})
            if proof_of_delivery:
                # Get delivery timestamp from proof of delivery
                pod_timestamp = proof_of_delivery.get('timestamp')
                if pod_timestamp and parsed_data['actualDelivery'] == 'N/A':
                    parsed_data['actualDelivery'] = pod_timestamp
                
                # Get receiver name from signed field
                signed_info = proof_of_delivery.get('signed', {})
                if signed_info:
                    receiver_name = (signed_info.get('name') or 
                                   signed_info.get('organizationName') or
                                   f"{signed_info.get('givenName', '')} {signed_info.get('familyName', '')}".strip())
                    if receiver_name:
                        parsed_data['receivedByName'] = receiver_name
            
            # Also check receiver information in details
            receiver_info = details.get('receiver', {})
            if receiver_info and parsed_data['receivedByName'] == 'N/A':
                receiver_name = (receiver_info.get('name') or 
                               receiver_info.get('organizationName') or
                               f"{receiver_info.get('givenName', '')} {receiver_info.get('familyName', '')}".strip())
                if receiver_name:
                    parsed_data['receivedByName'] = receiver_name
        
        # Parse events for additional pickup and delivery information
        events = shipment.get('events', [])
        
        # Sort events by timestamp to get the earliest pickup event
        sorted_events = sorted(events, key=lambda x: x.get('timestamp', ''), reverse=False)
        
        for event in sorted_events:
            event_status = event.get('status', '').lower()
            event_status_code = event.get('statusCode', '').lower()
            event_description = event.get('description', '').lower()
            
            # Enhanced pickup detection - look for multiple indicators
            pickup_keywords = [
                'pickup', 'pick up', 'collected', 'package received', 'label created',
                'shipment entered', 'received at', 'processed', 'awaiting shipment',
                'distribution center', 'origin', 'accepted', 'tendered'
            ]
            
            # Check for pickup events using multiple criteria
            is_pickup_event = (
                event_status_code == 'pre-transit' or
                any(keyword in event_status for keyword in pickup_keywords) or
                any(keyword in event_description for keyword in pickup_keywords) or
                'label created' in event_status or
                'package received' in event_status or
                'received at' in event_description
            )
            
            if is_pickup_event and parsed_data['actualPickup'] == 'N/A':
                parsed_data['actualPickup'] = event.get('timestamp', 'N/A')
            
            # Look for delivery events (delivered status code)
            if event_status_code == 'delivered':
                parsed_data['actualDelivery'] = event.get('timestamp', 'N/A')
                
                # Try to get location info for receiver name if not already set
                if parsed_data['receivedByName'] == 'N/A':
                    location = event.get('location', {})
                    if location:
                        address = location.get('address', {})
                        locality = address.get('addressLocality', '')
                        if locality:
                            parsed_data['receivedByName'] = locality
        
        # Check for error conditions (per API spec status codes)
        if status_info.get('statusCode') in ['unknown', 'failure']:
            parsed_data['isErrored'] = True
            parsed_data['errorDescription'] = (status_info.get('description') or 
                                             status_info.get('statusDetailed') or
                                             status_info.get('remark') or
                                             f"Shipment status: {status_info.get('status', 'unknown')}")
            
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

            transaction_id = str(uuid.uuid4())

            parsed_data = self.parse_tracking_fields(response)
            parsed_data['transactionId'] = transaction_id

        
            temp_df = pd.DataFrame([parsed_data], dtype='string')
            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

        
        # Add load timestamp to all rows
        combined_df['loadtimestamp'] = datetime.now().isoformat()
        '''
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        logging.info("dataframe is \n%s", combined_df)
        '''
        return combined_df

'''
if(__name__ == "__main__"):

    dhl_api_obj = DHLAPI()

    api_response = dhl_api_obj.curate_api_response(
        pd.DataFrame([
            {"TRACKINGID": "1102590775", "NotificationId": "test-notif-1"},
            {"TRACKINGID": "sreejittest1234manikanta", "NotificationId": "test-notif-2"},
        ]))

    final_output = dhl_api_obj.generate_final_output(api_response)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(final_output)
'''