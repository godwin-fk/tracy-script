import pandas as pd,os,json,requests
class CarrierUpdater:
    def __init__(self, csv_file_path):  
        self.csv_file_path = csv_file_path
        self.tracking_service_rr = 'https://tracking-api.fourkites.com'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': os.getenv('AUTH_TOKEN')
        }
        self.df = pd.read_csv(self.csv_file_path)
        self.carrier_names_dict = {}
        
    def search_shipments_with_pagination(self, shipment_numbers, shipper_id, graph_id):
            try:
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
             
    def update_carrier_name(self, load_responses):
        for load_details in load_responses:
            load_id = load_details.get("loadNumber", "")
            carrier_name = load_details.get("carrier", {}).get("name", "")
            self.carrier_names_dict[int(load_id)] = carrier_name
            print(load_id," ->> ",carrier_name)
        print(len(self.carrier_names_dict),"Loads Identified")
        self.df['carrier'] = self.df['load_id'].map(self.carrier_names_dict)
        return self.df
