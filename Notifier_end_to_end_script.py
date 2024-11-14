#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import datetime
import re
from queries import get_agentic_audit_logs_query
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.current_date = 'NOTIFIER_28thOCT_08thNOV'

    def fetch_data(self, db_url, query):
        try:
            conn = psycopg2.connect(db_url)
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            print(f"Error connecting to {db_url}: {e}")
        finally:
            if conn:
                conn.close()

    def clean_alert(self,alert):
        if isinstance(alert, float):
            alert = str(alert)
        if not alert:
            return ''
        
        cleaned_alert = re.sub(r'Load #\d+(?:[:])?\s*', '', alert)
        cleaned_alert = re.sub(r'[\\r\\n)]+$', '', cleaned_alert)
        return cleaned_alert
    
    def alter_change(self,filename):
        self.df['Alert'] = self.df['Alert'].apply(self.clean_alert)
        self.df.to_csv(filename, index=False)

        
    def add_response_time(self,filename):
        # Load the CSV file
        df = pd.read_csv(filename)

        # Convert Start Time and End Time columns to datetime format
        df['Start Time'] = pd.to_datetime(df['Start Time'], errors='coerce')
        df['End Time'] = pd.to_datetime(df['End Time'], errors='coerce')

        # Calculate Response Time in minutes
        df['Response Time'] = df.apply(
            lambda row: (row['End Time'] - row['Start Time']).total_seconds() / 60 if pd.notnull(row['End Time']) else None,
            axis=1
        )
        self.df = df
        # Save the updated CSV file
        self.df.to_csv(filename, index=False)

    def extract_fourkites_alert(self,alert):
        match = re.search(r'FourKites Alert:(.*)', str(alert))
        return match.group(1).strip() if match else alert
    
    def format_and_save_df(self,filename):
            # Rename the columns
            self.df = self.df.rename(columns={
                'load_id': 'Load Number',
                'request_id':'Request ID',
                'shipper_id': 'Shipper',
                'carrier': 'Carrier',
                'workflow': 'Workflow',
                'goal': 'Goal',
                'actions' :'Actions',
                'raw_message': 'Raw Response',
                'alert': 'Alert',
                'start_time': 'Start Time',
                'end_time': 'End Time'
            })
            
            # Rearrange the columns in the desired order
            self.df = self.df[['Load Number','Request ID', 'Shipper', 'Carrier', 'Workflow', 'Alert', 'Raw Response', 'Goal','Actions', 'Start Time', 'End Time']]
            
            self.df['Alert'] = self.df['Alert'].apply(self.extract_fourkites_alert)
            self.df.to_csv(filename, index=False)
            print(f"Final Data has been saved to {filename}.")


    def save_to_csv(self):
        # create directory path
        temp_dir = f'{os.path.dirname(os.path.abspath(__name__))}/files/'
        # create the directory
        os.makedirs(temp_dir, exist_ok=True)
        file_path = f"{temp_dir}tracy_{self.current_date}.csv"
        self.df.to_csv(file_path, index=False)
        print(f"Merged Data has been saved to {file_path}.")
        return file_path
        
    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        shipper_id = 'smithfield-foods'
        start_date = '2024-10-31'
        end_date = '2024-11-06'
        workflow_identifier = 'notifier'
        query = get_agentic_audit_logs_query(workflow_identifier,shipper_id, start_date, end_date)
        self.df = self.fetch_data(self.db1_url, query)

        filename = self.save_to_csv()
        
        carrier_updater = CarrierUpdater(filename)
        
        # Get shipment numbers from the DataFrame
        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]
        
        # Fetch carrier names from FourKites API
        load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
        self.df = carrier_updater.update_carrier_name(load_responses)

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)
        self.add_response_time(filename)
        self.alter_change(filename)


if __name__ == "__main__":
    main_process = Main()
    main_process.run()
