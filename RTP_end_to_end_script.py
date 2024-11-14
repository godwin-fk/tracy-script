#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import datetime
import re
from queries import query_db1, query_db2 ,get_query_1,get_query_2
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.current_date = 'RTP_28thOCT_08thNOV'

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

    def alter_change(self,filename):
        df = pd.read_csv(filename)
        df.to_csv(filename, index=False)

        
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

        # Save the updated CSV file
        df.to_csv(filename, index=False)
    
    def format_and_save_df(self, filename):
        # Rename the columns
        self.df = self.df.rename(columns={
            'load_id': 'Load Number',
            'request_id':'Request ID',
            'shipper_id': 'Shipper',
            'carrier': 'Carrier',
            'workflow': 'Workflow',
            'goal': 'Goal',
            'raw_message': 'Raw Response',
            'alert': 'Alert',
            'start_time': 'Start Time',
            'end_time': 'End Time',
            'actions ': 'Updated data points on FK',
            'reminder': 'Reminder',
            'escalated': 'Escalated'
        })
        
        # Rearrange the columns in the desired order
        self.df = self.df[['Load Number','Request ID', 'Shipper', 'Carrier', 'Workflow','Goal','Raw Response', 'Alert','Start Time','End Time','Updated data points on FK','Reminder', 'Escalated']]

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

    def extract_fourkites_alert(self,alert):
        match = re.search(r'FourKites Alert:(.*)', str(alert))
        return match.group(1).strip() if match else alert
    
    def process_data(self, df_db1, df_db2):
        # only alert value is extracted from the DF-1 alert column
        df_db1['alert'] = df_db1['alert'].apply(self.extract_fourkites_alert)
        
        df_db2['reminder'] = df_db2.apply(
            lambda row: 'Y' if row['status'] == 'response_received' and 'Escalation triggered' in row['comments'] else None,
            axis=1
        )
        
        df_db2.loc[df_db2['status'] == 'escalation_l2_sent', ['reminder', 'escalated']] = 'Y'
        
        # Drop unnecessary columns
        df_db2 = df_db2.drop(columns=['status', 'comments'])

        # Perform inner join on 'entity_id(load_id) and shipper_id'
        merged_df = pd.merge(df_db1, df_db2, on=['load_id', 'shipper_id'], how='inner')
        
        # Drop duplicates if any
        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.fillna('')
        
        return merged_df
        
    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        shipper_id = 'smithfield-foods'
        start_date = '2024-10-31'
        end_date = '2024-11-06'
        workflow_identifier = 'ready_to_pickup'
        query_1 = get_query_1(workflow_identifier,shipper_id, start_date, end_date)
        query_2 = get_query_2(shipper_id, start_date, end_date)
        df_db1 = self.fetch_data(self.db1_url, query_1)
        df_db2 = self.fetch_data(self.db2_url, query_2)
        # here self.df is the merged data of df1 and df2
        self.df = self.process_data(df_db1, df_db2)
        # save to file: 
        filename = self.save_to_csv()

        carrier_updater = CarrierUpdater(filename)
        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]

        load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
        self.df = carrier_updater.update_carrier_name(load_responses)

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)


        self.add_response_time(filename)
        # self.alter_change(filename)


if __name__ == "__main__":
    main_process = Main()
    main_process.run()
