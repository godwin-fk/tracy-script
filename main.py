#!/usr/bin/env python3
import os
import psycopg2
import pandas as pd
import datetime
from queries import query_db1, query_db2  
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.current_date = datetime.datetime.now().strftime('%Y-%m-%d')

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
   
    def process_data(self, df_db1, df_db2):
        '''add columns and merge dataframes.'''
        # Convert 'start_time' and 'end_time' to datetime to avoid errors
        df_db1['start_time'] = pd.to_datetime(df_db1['start_time'], errors='coerce')
        df_db1['end_time'] = pd.to_datetime(df_db1['end_time'], errors='coerce')
        
        # Group by 'entity_id' and fill missing values with the correct start time
        df_db1['start_time'] = df_db1.groupby('load_id')['start_time'].transform(lambda x: x.ffill())
         
        # Additional logics to create new columns based on the existing data
        df_db1['date'] = self.current_date
        
        df_db1['response_time'] = df_db1['end_time'] - df_db1['start_time']
        
        df_db2['reminder'] = df_db2.apply(
            lambda row: 'Y' if row['status'] == 'response_received' and 'Escalation triggered' in row['comments'] else None,
            axis=1
        )
        
        df_db2.loc[df_db2['status'] == 'escalation_l2_sent', ['reminder', 'escalated']] = 'Y'
        
        # Drop unnecessary columns
        df_db1 = df_db1.drop(columns=['start_time', 'end_time',])
        df_db2 = df_db2.drop(columns=['status', 'comments'])

        # Perform join on 'entity_id(load_id)'
        merged_df = pd.merge(df_db1, df_db2, on='load_id', how='left')
        
        # Drop duplicates if any
        merged_df = merged_df.drop_duplicates()
        
        return merged_df

    def save_to_csv(self):
        # Save the merged data to a CSV file
        temp_dir = f'{os.path.dirname(os.path.abspath(__name__))}/files/'
        os.makedirs(temp_dir, exist_ok=True)
        file_path = f"{temp_dir}tracy_{self.current_date}.csv"
        self.df.to_csv(file_path, index=False)
        print(f"Merged Data has been saved to {file_path}.")
        return file_path
    
    def format_and_save_df(self, filename):
        # Rename the columns
        self.df = self.df.rename(columns={
            'date': 'Date',
            'load_id': 'Load Number',
            'shipper_id': 'Shipper',
            'carrier': 'Carrier',
            'response_time': 'Response Time',
            'Actions': 'Updated data points on FK',
            'reminder': 'Reminder',
            'escalated': 'Escalated',
        })
        
        # Rearrange the columns in the desired order
        self.df = self.df[['Date', 'Load Number', 'Shipper', 'Carrier', 'Workflow', 'Response Time', 'Goal', 'Updated data points on FK', 'Reminder', 'Escalated']]
        
        # Save the updated DataFrame to a CSV file
        self.df.to_csv(filename, index=False)
        print(f"Final Data has been saved to {filename}.")
        
    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        df_db1 = self.fetch_data(self.db1_url, query_db1)
        df_db2 = self.fetch_data(self.db2_url, query_db2)

        # Process and merge the data
        self.df = self.process_data(df_db1, df_db2)

        # Save the merged data to CSV and get the filename
        filename = self.save_to_csv()
        
        carrier_updater = CarrierUpdater(filename)
        self.df = carrier_updater.update_carrier_names()
        self.format_and_save_df(filename)


if __name__ == "__main__":
    main_process = Main()
    main_process.run()
