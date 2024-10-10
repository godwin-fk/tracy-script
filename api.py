import os
import requests
import pandas as pd

class CarrierUpdater:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.headers =  {
            'Content-Type': 'application/json',
            'Authorization': os.getenv("AUTH_TOKEN")
        }
        self.df = pd.read_csv(self.csv_file_path)
        self.carrier_names = []

    def fetch_carrier_name(self, shipper_id, load_id):
        url = f'https://tracking-api.fourkites.com/api/v1/tracking/search?company_id={shipper_id}&show=stops'
        data = {"load_ids": load_id}

        # Make the GET request
        response = requests.get(url, headers=self.headers, json=data)

        if response.status_code == 200:
            store = response.json()
            if "loads" in store and len(store["loads"]) > 0:
                return store["loads"][0]["carrier"]["name"]
        return None

    def update_carrier_names(self):
        print("Updating Carrier names in the CSV file...")
        for index, row in self.df.iterrows():
            shipper_id = row['shipper_id']
            load_id = row['load_id']
            carrier_name = self.fetch_carrier_name(shipper_id, load_id)
            print(f"Carrier Name for Load_id {load_id}: {carrier_name}")
            self.carrier_names.append(carrier_name)

        self.df['carrier'] = self.carrier_names
        print("Carrier names have been updated in the CSV file.")
        return self.df
        
       
    # def save_csv(self):
    #     self.df.to_csv(self.csv_file_path, index=False)
    #     print("Carrier names have been updated in the CSV file.")
