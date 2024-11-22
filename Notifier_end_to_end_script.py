#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
from datetime import datetime
import re
from queries import get_agentic_audit_logs_query,get_milestones_query
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()

class Main:
    def __init__(self,shipper_id, start_date, end_date,workflow_identifier,current_date):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        #milestones db url
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.shipper_id = shipper_id
        self.start_date = start_date
        self.end_date = end_date
        self.workflow_identifier = workflow_identifier
        self.current_date = current_date

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

    def clean_alert(self,trigger_message):
        if isinstance(trigger_message, float):
            trigger_message = str(trigger_message)
        if not trigger_message:
            return ''
        
        cleaned_alert = re.sub(r'Load #\d+(?:[:])?\s*', '', trigger_message)
        cleaned_alert = re.sub(r'[\\r\\n)]+$', '', cleaned_alert)
        return cleaned_alert

    def alter_change(self,filename):
        self.df['Trigger Message'] = self.df['Trigger Message'].apply(self.clean_alert)
        self.df.to_csv(filename, index=False)

        
    def add_response_time(self,filename):
        # Load the CSV file
        df = pd.read_csv(filename)

        # Convert Triggered At and Response At columns to datetime format
        df['Enquiry Sent At'] = pd.to_datetime(df['Enquiry Sent At'], errors='coerce')
        df['Response At'] = pd.to_datetime(df['Response At'], errors='coerce')

        # Calculate Response Delay (mins) in minutes
        df['Response Delay (mins)'] = df.apply(
            lambda row: (row['Response At'] - row['Enquiry Sent At']).total_seconds() / 60 if pd.notnull(row['Response At']) else None,
            axis=1
        )
        self.df = df
        self.df = self.df[['Workflow','Workflow Execution Id','Shipper', 'Carrier','Carrier SCAC','Load Number','Trigger Message','Response Message','Triggered At','Enquiry Sent At','Response At','Response Delay (mins)','Update Actions','Status','Reminder','Escalated']]
        # Save the updated CSV file
        self.df.to_csv(filename, index=False)

    def extract_fourkites_alert(self,trigger_message):
        match = re.search(r'FourKites Alert:(.*)', str(trigger_message))
        return match.group(1).strip() if match else trigger_message
    
    def format_and_save_df(self,filename):
            # Rename the columns
            self.df = self.df.rename(columns={
                'workflow': 'Workflow',
                'load_id': 'Load Number',
                'workflow_exec_id': 'Workflow Execution Id',
                'shipper_id': 'Shipper',
                'carrier': 'Carrier',
                'scac' : 'Carrier SCAC',
                'status': 'Status',
                'actions' :'Update Actions',
                'response_message': 'Response Message',
                'trigger_message': 'Trigger Message',
                'trigger_timestamp': 'Triggered At',
                'response_timestamp': 'Response At',
                'reminder': 'Reminder',
                'escalated': 'Escalated',
                'enquiry_sent_at': 'Enquiry Sent At'
            })
            # TODO:add - in single string
            self.df['Workflow Execution Id'] = self.df.apply(
                lambda row: f"{row['Workflow Execution Id']}-{row['Load Number']}", axis=1
            )

            # Rearrange the columns in the desired order
            # self.df = self.df[['Workflow Execution Id','Load Number','Shipper', 'Carrier','Carrier SCAC','Workflow','Update Actions','Response Message','Triggered At', 'Response At','Status','Trigger Message','Reminder','Escalated']]
            
            self.df['Trigger Message'] = self.df['Trigger Message'].apply(self.extract_fourkites_alert)
            self.df.to_csv(filename, index=False)
            print(f"Final Data has been saved to {filename}.")


    def save_to_csv(self):
        # create directory path
        temp_dir = f'{os.path.dirname(os.path.abspath(__name__))}/files/'
        # create the directory
        os.makedirs(temp_dir, exist_ok=True)
        file_path = f"{temp_dir}{self.current_date}"
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
        self.df = pd.merge(df1, df_db2, on=['load_id', 'shipper_id'], how='left')
        self.df['enquiry_sent_at'] = pd.to_datetime(self.df['enquiry_sent_at'])
        self.df['trigger_timestamp'] = pd.to_datetime(self.df['trigger_timestamp'])

        # remove rows & update status as TRIGGER_SKIPPED as applicable
        requests_processed = set()
        rows_to_remove = set()
        for index, row in self.df.iterrows():
            if not pd.isnull(row['enquiry_sent_at']) and (row['enquiry_sent_at'] < row['trigger_timestamp'] or (row['enquiry_sent_at'] - row['trigger_timestamp']).total_seconds() > 120) :
                rows_to_remove.add(index)
                self.df.at[index, 'status'] = 'TRIGGER_SKIPPED'
                self.df.at[index, 'response_message'] = None
                self.df.at[index, 'response_timestamp'] = None
                self.df.at[index, 'actions'] = 'DETAILS_EXTRACTED'
            else:
                requests_processed.add(row['workflow_exec_id'])
                if pd.isnull(row['enquiry_sent_at']):
                    self.df.at[index, 'status'] = 'TRIGGER_SKIPPED'
                    self.df.at[index, 'response_message'] = None
                    self.df.at[index, 'response_timestamp'] = None
                    self.df.at[index, 'actions'] = 'DETAILS_EXTRACTED'

        for index, row in self.df.iterrows():
            if row['workflow_exec_id'] not in requests_processed:
                requests_processed.add(row['workflow_exec_id'])
                rows_to_remove.remove(index)

        self.df = self.df.drop(index=rows_to_remove).reset_index(drop=True)
        self.df = self.df.drop_duplicates()
        self.df = self.df.fillna('')


    def run(self):
        """Fetch, process, and save the data."""
        query = get_agentic_audit_logs_query(self.workflow_identifier,self.shipper_id, self.start_date, self.end_date)
        self.df = self.fetch_data(self.db1_url, query)
        #milestones query
        milestone_query = get_milestones_query(self.shipper_id, self.workflow_identifier, self.start_date, self.end_date)
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


def convert_date_to_custom_format(date_str):
    # Parse the date string
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Extract day and month
    day = date_obj.day
    month_abbr = date_obj.strftime('%b').upper()  # Get abbreviated month in uppercase
    
    # Determine day suffix
    if 11 <= day <= 13:  # Handle special cases for 11th, 12th, and 13th
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    
    # Construct the custom format
    custom_format = f"{day}{suffix}{month_abbr}"
    return custom_format

if __name__ == "__main__":
    shipper_id = 'smithfield-foods'
    start_date = '2024-10-31'
    end_date = '2024-11-20'
    workflow_identifier = 'notifier'
    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    year = date_obj.year
    current_date = f'notifier_report_{convert_date_to_custom_format(start_date)}_{convert_date_to_custom_format(end_date)}{year}.csv'
    main_process = Main(shipper_id, start_date, end_date,workflow_identifier,current_date)
    main_process.run()
