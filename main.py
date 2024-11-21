#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import datetime
import re
from queries import query_db1, query_db2  
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.current_date = '28thOCT_08thNOV'

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
                
    def extract_fourkites_alert(self,trigger_message):
        match = re.search(r'FourKites Alert:(.*)', str(trigger_message))
        return match.group(1).strip() if match else trigger_message
   
    def process_data(self, df_db1, df_db2):
        
        # Convert 'trigger_timestamp' and 'response_timestamp' to datetime to avoid errors
        df_db1['trigger_timestamp'] = pd.to_datetime(df_db1['trigger_timestamp'], errors='coerce')
        df_db1['response_timestamp'] = pd.to_datetime(df_db1['response_timestamp'], errors='coerce')
        
        # Group by 'entity_id' and fill missing values with the correct start time
        df_db1['trigger_timestamp'] = df_db1.groupby('load_id')['trigger_timestamp'].transform('min')
        df_db1['response_timestamp'] = df_db1.groupby('load_id')['response_timestamp'].transform('min')
        
        df_db1['response_time'] = df_db1['response_timestamp'] - df_db1['trigger_timestamp']
        df_db1['response_time'] = df_db1['response_time'].apply(lambda x: x if x >= pd.Timedelta(0) else 'NaN')
        
        
        df_db1['start_date'] = pd.to_datetime(df_db1['trigger_timestamp'], format='%Y-%m-%d')
        df_db1['end_date'] = pd.to_datetime(df_db1['response_timestamp'], format='%Y-%m-%d')

        df_db1['Trigger Message'] = df_db1['Trigger Message'].apply(self.extract_fourkites_alert)
        
        df_db1['Response Message'] = df_db1['Response Message'].str.replace('Raw message:', '', regex=False)
        
        df_db2['reminder'] = df_db2.apply(
            lambda row: 'Y' if row['status'] == 'response_received' and 'Escalation triggered' in row['comments'] else None,
            axis=1
        )
        
        df_db2.loc[df_db2['status'] == 'escalation_l2_sent', ['reminder', 'escalated']] = 'Y'
        
        # Drop unnecessary columns
        df_db1 = df_db1.drop(columns=['trigger_timestamp', 'response_timestamp',])
        df_db2 = df_db2.drop(columns=['status', 'comments'])

        # Perform inner join on 'entity_id(load_id) and shipper_id'
        merged_df = pd.merge(df_db1, df_db2, on=['load_id', 'shipper_id'], how='inner')
        
        # Drop duplicates if any
        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.fillna('')
        
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
            'start_date': 'Triggered At',
            'end_date': 'Response At',
            'load_id': 'Load Number',
            'shipper_id': 'Shipper',
            'carrier': 'Carrier',
            'response_time': 'Response Delay (mins)',
            'actions': 'Update Actions',
            'reminder': 'Reminder',
            'escalated': 'Escalated',
        })
        
        # Rearrange the columns in the desired order
        self.df = self.df[['Triggered At','Response At', 'Load Number', 'Shipper', 'Carrier', 'Workflow', 'Trigger Message', 'Response Delay (mins)', 'Response Message', 'Status', 'Update Actions','Reminder', 'Escalated']]
        # Step 1: Convert Triggered At and Response At to datetime and sort by Triggered At in ascending order
        self.df['Triggered At'] = pd.to_datetime(self.df['Triggered At'])
        self.df['Response At'] = pd.to_datetime(self.df['Response At'])
        self.df = self.df.sort_values(by='Triggered At')

        # Step 2: Remove timestamp, keeping only the date part
        self.df['Triggered At'] = self.df['Triggered At'].dt.date
        self.df['Response At'] = self.df['Response At'].dt.date
        self.df.reset_index(drop=True, inplace=True)
        # Convert Response Delay (mins) to minutes
        def convert_to_minutes(response_time):
            if pd.isna(response_time) or response_time == '':
                return ''
            
            days, time_part = response_time.split(' days ')
            hours, minutes, _ = map(int, time_part.split(':'))
            total_minutes = int(days) * 1440 + hours * 60 + minutes  # 1440 = 24*60
            return total_minutes
        self.df['Response Delay (mins)'] = self.df['Response Delay (mins)'].apply(convert_to_minutes)
        self.df.to_csv(filename, index=False)
        print(f"Final Data has been saved to {filename}.")
        
    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        df_db1 = self.fetch_data(self.db1_url, query_db1)

        df_db2 = self.fetch_data(self.db2_url, query_db2)

        # Process and merge the data
        self.df = self.process_data(df_db1, df_db2)

        # Save the merged data to CSV and get the filename before updating carrier names
        filename = self.save_to_csv()
        
        carrier_updater = CarrierUpdater(filename)
        
        # Get shipment numbers from the DataFrame
        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]
        
        # Fetch carrier names from FourKites API
        load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, 'smithfield-foods', 'graph_id')
        self.df = carrier_updater.update_carrier_name(load_responses)

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)


if __name__ == "__main__":
    main_process = Main()
    main_process.run()
