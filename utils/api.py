import pandas as pd
import os
import json
import requests

class CarrierUpdater:
    def __init__(self, csv_file_path):  
        self.csv_file_path = csv_file_path
        self.tracking_service_rr = 'https://tracking-api.fourkites.com'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': os.getenv('AUTH_TOKEN')
        }
        self.df = pd.read_csv(self.csv_file_path)
        self.carrier_info_dict = {}  # Updated to store carrier_name and scac
        
    def search_shipments_with_pagination(self, shipment_numbers, shipper_id, graph_id):
        try:
            shipment_numbers = list(set(shipment_numbers))
            get_load_url = f'{self.tracking_service_rr}/api/v1/tracking/search?company_id={shipper_id}&show=stops'
            load_response_final = []
            with requests.Session() as session:
                session.headers.update(self.headers)
                for i in range(0, len(shipment_numbers), 10):
                    payload = json.dumps({"load_ids": ",".join(shipment_numbers[i:i+10])})
                    load_response = session.get(get_load_url, data=payload)
                    if load_response.status_code != 200:
                        failure_log = f"Failed to get carrier scac for loads: {shipment_numbers}. Error: {load_response.text}"
                        raise Exception(failure_log)
                    load_response_final.extend(load_response.json().get("loads", []))
                return load_response_final
        except Exception as e:
            raise Exception(f"Failed to get {shipper_id} shipment details. Error: {str(e)}") 
             
    # RTP: Update carrier info
    def update_carrier_info(self,load_responses):
        if(len(load_responses) > 0):
            for load_details in load_responses:
                load_id = load_details.get("loadNumber", "")
                carrier_name = load_details.get("carrier", {}).get("name", "")
                scac = load_details.get("carrier", {}).get("id", "")  # Extract SCAC
                self.carrier_info_dict[int(load_id)] = {
                    "carrier": carrier_name,
                    "scac": scac
                }
                print(load_id, " ->> ", {"carrier": carrier_name, "scac": scac})
            self.df['carrier'] = self.df['Load Number'].map(
                lambda load_id: self.carrier_info_dict.get(load_id, {}).get('carrier', "")
            )
            self.df['scac'] = self.df['Load Number'].map(
                lambda load_id: self.carrier_info_dict.get(load_id, {}).get('scac', "")
            )
        else:    
            self.df['carrier']=''
            self.df['scac']=''

        return self.df
    

    # Notifier: Update carrier info
    def update_carrier_info_v2(self,load_responses):
        if(len(load_responses) > 0):
            for load_details in load_responses:
                load_id = load_details.get("loadNumber", "")
                carrier_name = load_details.get("carrier", {}).get("name", "")
                scac = load_details.get("carrier", {}).get("id", "")  # Extract SCAC
                self.carrier_info_dict[int(load_id)] = {
                    "carrier": carrier_name,
                    "scac": scac
                }
                print(load_id, " ->> ", {"carrier": carrier_name, "scac": scac})
            self.df['carrier'] = self.df['load_id'].map(
                lambda load_id: self.carrier_info_dict.get(load_id, {}).get('carrier', "")
            )
            self.df['scac'] = self.df['load_id'].map(
                lambda load_id: self.carrier_info_dict.get(load_id, {}).get('scac', "")
            )
        else:    
            self.df['carrier']=''
            self.df['scac']=''

        return self.df
      
