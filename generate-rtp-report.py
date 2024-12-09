#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import csv
from datetime import datetime
import re
from utils.queries import get_rtp_agentic_audit_logs_query,get_milestones_query
from utils.api import CarrierUpdater
from utils.get_holdover import GmailDataProcessor
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self,shipper_id, start_date, end_date,workflow_identifier,holdover,output_csv_path,flag):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.shipper_id = shipper_id
        self.start_date = start_date
        self.end_date = end_date
        self.workflow_identifier = workflow_identifier
        self.holdover = holdover
        self.output_csv_path = output_csv_path
        self.current_date = f"{self.workflow_identifier}_{start_date}_{self.end_date}"
        self.flag = flag

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
        
    def add_response_time(self,filename):
        # Load the CSV file
        df = pd.read_csv(filename)

        # Convert Triggered At and Response At columns to datetime format
        df['Followup Sent At'] = pd.to_datetime(df['Followup Sent At'], errors='coerce')
        df['Response At'] = pd.to_datetime(df['Response At'], errors='coerce')

        # Calculate Response Delay (mins) in minutes
        df['Response Delay (mins)'] = df.apply(
            lambda row: (row['Response At'] - row['Followup Sent At']).total_seconds() / 60 if pd.notnull(row['Response At']) else None,
            axis=1
        )
        df = df[['Load Number','CARRIER','CONTAINER ID','DESTINATION CITY','DESTINATION STATE','DD DATE','DD TIME','BILL DATE','BILL TIME','ZDLT LATE CODE','ON TIME? (Y/N)','SPLIT? (Y/N)','FRESH PRIORITY STO? (Y/N)','NOTES/COMMENTS', 'FACILITY', 'DATE' ,'Workflow','Workflow Execution Id','Shipper', 'Carrier','Carrier SCAC','Trigger Message','Response Message','Triggered At','Followup Sent At','Response At','Response Delay (mins)','Update Actions','Status','Reminder','Escalated']]
    
        self.df = df
        # Save the updated CSV file
        self.df.to_csv(filename, index=False)
    
    def format_and_save_df(self, filename):
        # Rename the columns

        self.df = self.df.rename(columns={
            'workflow_exec_id': 'Workflow Execution Id',
            'shipper_id': 'Shipper',
            'carrier': 'Carrier',
            'scac' : 'Carrier SCAC',
            'workflow': 'Workflow',
            'status': 'Status',
            'response_message': 'Response Message',
            'trigger_message': 'Trigger Message',
            'trigger_timestamp': 'Triggered At',
            'response_timestamp': 'Response At',
            'actions': 'Update Actions',
            'reminder': 'Reminder',
            'escalated': 'Escalated',
            'followup_sent_at': 'Followup Sent At'
        })

        self.df['Triggered At'] = pd.to_datetime(self.df['Triggered At'])
        self.df=self.df.sort_values(by=['Triggered At', 'Load Number'])
        self.df.to_csv(filename, index=False)
        print(f"Mapping of headers is done and saved to : {filename}.")

    def save_to_csv(self):
        file_path = f"./tmp/tracy_{self.current_date}.csv"
        self.df.to_csv(file_path, index=False)
        print(f"Merged Data has been saved to {file_path}.")
        return file_path

    def extract_fourkites_alert(self,trigger_message):
        match = re.search(r'FourKites Alert:(.*)', str(trigger_message))
        return match.group(1).strip() if match else trigger_message
    
    def process_data(self, df_db1, df_db2):
        # only trigger_message value is extracted from the DF-1 trigger_message column
        df_db1['trigger_message'] = df_db1['trigger_message'].apply(self.extract_fourkites_alert)
        df_db2['reminder'] = df_db2.apply(
            lambda row: 'Y' if row['status'] == 'response_received' and 'Escalation triggered' in row['comments'] else None,
            axis=1
        )
        df_db2.loc[df_db2['status'] == 'escalation_l2_sent', ['reminder', 'escalated']] = 'Y'


        if 'status' and 'comments' in df_db2.columns:
            df_db2 = df_db2.drop(columns=['status', 'comments'])
        else:
            print('Columns empty in df_db2')

        self.df= pd.merge(df_db1, df_db2, on=['load_id'], how='left')

        self.df['followup_sent_at'] = pd.to_datetime(self.df['followup_sent_at'])
        self.df['trigger_timestamp'] = pd.to_datetime(self.df['trigger_timestamp'])

        # remove rows & update status as TRIGGER_SKIPPED as applicable
        requests_processed = set()
        rows_to_remove = set()
        for index, row in self.df.iterrows():
            if not pd.isnull(row['followup_sent_at']) and (row['followup_sent_at'] < row['trigger_timestamp'] or (row['followup_sent_at'] - row['trigger_timestamp']).total_seconds() > 120) :
                rows_to_remove.add(index)
                self.df.at[index, 'status'] = 'TRIGGER_SKIPPED'
                self.df.at[index, 'response_message'] = None
                self.df.at[index, 'response_timestamp'] = None
                self.df.at[index, 'actions'] = 'DETAILS_EXTRACTED'
            else:
                requests_processed.add(row['workflow_exec_id'])
                if pd.isnull(row['followup_sent_at']):
                    self.df.at[index, 'status'] = 'TRIGGER_SKIPPED'
                    self.df.at[index, 'response_message'] = None
                    self.df.at[index, 'response_timestamp'] = None
                    self.df.at[index, 'actions'] = 'DETAILS_EXTRACTED'

        for index, row in self.df.iterrows():
            if row['workflow_exec_id'] not in requests_processed:
                requests_processed.add(row['workflow_exec_id'])
                rows_to_remove.remove(index)

        self.df = self.df.drop(index=rows_to_remove).reset_index(drop=True)
        self.df = self.df.drop(columns=["shipper_id_y"]).rename(columns={"shipper_id_x": "shipper_id"})
        self.df = self.df.drop_duplicates()
        self.df = self.df.fillna('')


    def join(self,holdover,filename,join_column='Load Number', how='left'):
        # Load the CSV files into DataFrames
        df1 = pd.read_csv(holdover,dtype={join_column: 'Int64'})
        df2 = pd.read_csv(filename,dtype={'load_id': 'Int64'})
        unnamed_cols = df1.columns[df1.columns.str.startswith('Unnamed')]
        if len(unnamed_cols) >= 2:
            df1.rename(columns={unnamed_cols[-2]: 'FACILITY', unnamed_cols[-1]: 'DATE'}, inplace=True)

        requests_processed = set()
        rows_to_remove = set()
        for index, row in df2.iterrows():
            if row['status'] == 'TRIGGER_SKIPPED':
                rows_to_remove.add(index)
            else:
                requests_processed.add(row['workflow_exec_id'])

        for index, row in df2.iterrows():
            if row['workflow_exec_id'] not in requests_processed:
                requests_processed.add(row['workflow_exec_id'])
                rows_to_remove.remove(index)

        df2 = df2.drop(index=rows_to_remove).reset_index(drop=True)
        df2 = df2.drop_duplicates(subset='load_id', keep='first')

        # Perform the join on the specified column
        result = pd.merge(df1, df2, left_on=join_column, right_on='load_id', how=how)
        # output_file = f'./dist/{self.shipper_id}-report-{self.start_date}_{self.end_date}.csv'
        result.to_csv(self.output_csv_path, index=False)
        print('The JOIN is completed')
        self.df = result
        return self.output_csv_path
        
    def fill(self,filename):
        # Load the CSV file
        df = pd.read_csv(filename)

        # Update the 'NOTES/COMMENTS' column based on conditions
        df['NOTES/COMMENTS'] = df.apply(
            lambda row: 'Email not sent' if pd.isnull(row['Followup Sent At'])
            else 'Email sent and awaiting response' if pd.isnull(row['Response At'])
            else 'Email sent and response processed' if '_UPDATED' in row['Update Actions'].upper()
            else 'Email sent and response not processed',
            axis=1
        )

        # Save the updated DataFrame back to CSV
        self.df = df
        self.df.to_csv(filename, index=False)
        print(f'NOTES/COMMENTS updated and saved to: {filename}')

  

    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        query_1 = get_rtp_agentic_audit_logs_query(self.workflow_identifier,self.shipper_id, self.start_date, self.end_date)
        df_db1 = self.fetch_data(self.db1_url, query_1)

        query_2 = get_milestones_query(self.shipper_id, self.workflow_identifier, self.start_date, self.end_date)
        df_db2 = self.fetch_data(self.db2_url, query_2)
        
        # here self.df is the merged data of df1 and df2
        self.process_data(df_db1, df_db2)
        # save to file: 
        filename = self.save_to_csv()
        output_file = self.join(self.holdover,filename)
        file_df = pd.read_csv(output_file)
        if 'load_id' in file_df.columns:
            file_df = file_df.drop(columns=['load_id'])
        self.df = file_df
        self.df.to_csv(output_file, index=False)

        # Carrier Updater based on flag : 
        carrier_updater = CarrierUpdater(output_file)

        if(self.flag):
            shipment_numbers = self.df['Load Number'].tolist()
            shipment_numbers = [str(x) for x in shipment_numbers]

            load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
            
        else:
            load_responses = []

        self.df = carrier_updater.update_carrier_info(load_responses)
        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(output_file)
        self.add_response_time(output_file)
        self.fill(output_file)


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
    # Define the parameters
    shipper_id = 'smithfield-foods'
    agent_id = 'TRACY'
    start_date = '2024-12-05'
    end_date = '2024-12-05'
    workflow_identifier = 'ready_to_pickup'

    output_file = "./tmp/output_data.xlsx"
    save_path="./tmp/attachments"
    
    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    year = date_obj.year
    output_csv_path = f'./dist/{shipper_id}-rtp-report_{convert_date_to_custom_format(start_date)}_{convert_date_to_custom_format(end_date)}{year}.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    
    processor = GmailDataProcessor(shipper_id, agent_id)
    processor.process_emails(
        save_path=save_path,
        output_file=output_file,
        start_date=start_date,
        end_date=end_date
    )
    
    #From tmp folder after processing the emails:
    holdover_shipper_id = shipper_id.replace('-', '_')
    holdover = f"./tmp/{holdover_shipper_id}-holdover-{start_date}_{end_date}.csv"
    print('The holdover: ',holdover)
    
    flag=True
    main_process = Main(
        shipper_id, 
        start_date, 
        end_date, 
        workflow_identifier, 
        holdover, 
        output_csv_path, 
        flag
    )
    main_process.run()