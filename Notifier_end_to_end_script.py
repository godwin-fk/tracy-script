#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import datetime
import re
from queries import get_agentic_audit_logs_query,get_milestones_query
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self,shipper_id, start_date, end_date,workflow_identifier):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        #milestones db url
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.shipper_id = shipper_id
        self.start_date = start_date
        self.end_date = end_date
        self.workflow_identifier = workflow_identifier
        self.current_date = f"{self.workflow_identifier}_{start_date}_{self.end_date}"

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
        self.df = self.df[['Workflow','Shipper', 'Carrier','SCAC','Load Number','Workflow Execution Id','Alert','Raw Response','Response Time','Actions','Status','Start Time','End Time','Reminder','Escalated']]
        # Save the updated CSV file
        self.df.to_csv(filename, index=False)

    def extract_fourkites_alert(self,alert):
        match = re.search(r'FourKites Alert:(.*)', str(alert))
        return match.group(1).strip() if match else alert
    
    def format_and_save_df(self,filename):
            # Rename the columns
            self.df = self.df.rename(columns={
                'workflow': 'Workflow',
                'load_id': 'Load Number',
                'request_id': 'Workflow Execution Id',
                'shipper_id': 'Shipper',
                'carrier': 'Carrier',
                'scac' : 'SCAC',
                'goal': 'Status',
                'actions' :'Actions',
                'raw_message': 'Raw Response',
                'alert': 'Alert',
                'start_time': 'Start Time',
                'end_time': 'End Time',
                'reminder': 'Reminder',
                'escalated': 'Escalated'
            })
            # add - in single string
            self.df['Workflow Execution Id'] = self.df.apply(
                lambda row: f"({row['Workflow Execution Id']} , {row['Load Number']})", axis=1
            )

            # Rearrange the columns in the desired order
            self.df = self.df[['Workflow','Shipper', 'Carrier','SCAC','Load Number','Workflow Execution Id','Alert','Raw Response','Actions','Status','Start Time', 'End Time','Reminder','Escalated']]
            
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

    def process_data(self, df1, df_db2):
        df_db2['reminder'] = df_db2.apply(
            lambda row: 'Y' if row['status'] == 'response_received' and 'Escalation triggered' in row['comments'] else None,
            axis=1
        )
        
        df_db2.loc[df_db2['status'] == 'escalation_l2_sent', ['reminder', 'escalated']] = 'Y'
        
        # Drop unnecessary columns
        df_db2 = df_db2.drop(columns=['status', 'comments'])
        self.df = pd.merge(df1, df_db2, on=['load_id', 'shipper_id'], how='inner')
        self.df = self.df.drop_duplicates()
        self.df = self.df.fillna('')


    def run(self):
        """Fetch, process, and save the data."""
        query = get_agentic_audit_logs_query(self.workflow_identifier,self.shipper_id, self.start_date, self.end_date)
        self.df = self.fetch_data(self.db1_url, query)
        #milestones query
        milestone_query = get_milestones_query(self.shipper_id, self.start_date, self.end_date)
        df2 = self.fetch_data(self.db2_url, milestone_query)
        #merge agentic audit logs and milestones :
        self.process_data(self.df, df2)
        filename = self.save_to_csv()
        
        carrier_updater = CarrierUpdater(filename)
        
        # Get shipment numbers from the DataFrame
        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]
        
        # Fetch carrier names from FourKites API
        load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
        self.df = carrier_updater.update_carrier_info(load_responses)

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)
        self.add_response_time(filename)
        self.alter_change(filename)


if __name__ == "__main__":
    shipper_id = 'smithfield-foods'
    start_date = '2024-10-31'
    end_date = '2024-11-06'
    workflow_identifier = 'notifier'
    main_process = Main(shipper_id, start_date, end_date,workflow_identifier)
    main_process.run()
