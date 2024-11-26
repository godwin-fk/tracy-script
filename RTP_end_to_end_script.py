#!/usr/bin/env python3
import os,re
import psycopg2
import pandas as pd
import csv
from datetime import datetime
import re
from queries import get_agentic_audit_logs_query,get_milestones_query
from api import CarrierUpdater
from dotenv import load_dotenv
load_dotenv()
class Main:
    def __init__(self,shipper_id, start_date, end_date,workflow_identifier,holdover,log_csv_path,output_csv_path):
        self.df = None
        self.db1_url = os.getenv("AGENTICAUDITLOGS_DB_URL")
        self.db2_url = os.getenv("MILESTONES_DB_URL")
        self.shipper_id = shipper_id
        self.start_date = start_date
        self.end_date = end_date
        self.workflow_identifier = workflow_identifier
        self.holdover = holdover
        self.log_csv_path = log_csv_path
        self.output_csv_path = output_csv_path
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

    def alter_change(self,filename):
        df = pd.read_csv(filename)
        df.to_csv(filename, index=False)

        
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
        df = df[['Workflow','Workflow Execution Id','Shipper', 'Carrier','Carrier SCAC','Load Number','Trigger Message','Response Message','Triggered At','Enquiry Sent At','Response At','Response Delay (mins)','Update Actions','Status','Reminder','Escalated']]
        # self.df[['Workflow','Workflow Execution Id','Shipper', 'Carrier','Carrier SCAC','Load Number','Trigger Message','Response Message','Triggered At','Enquiry Sent At','Response At','Response Delay (mins)','Update Actions','Status','Reminder','Escalated']]
       
        # Save the updated CSV file
        df.to_csv(filename, index=False)
    
    def format_and_save_df(self, filename):
        # Rename the columns
        self.df = self.df.rename(columns={
            'load_id': 'Load Number',
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
            'enquiry_sent_at': 'Enquiry Sent At'
        })

        self.df['Triggered At'] = pd.to_datetime(self.df['Triggered At'])
        self.df=self.df.sort_values(by=['Triggered At', 'Load Number'])
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
        # Drop unnecessary columns
        df_db2 = df_db2.drop(columns=['status', 'comments'])

        self.df= pd.merge(df_db1, df_db2, on=['load_id'], how='left')

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
        self.df = self.df.drop(columns=["shipper_id_y"]).rename(columns={"shipper_id_x": "shipper_id"})
        self.df = self.df.drop_duplicates()
        self.df = self.df.fillna('')


    def join(self,holdover,filename,join_column='Load Number', how='left'):
        # Load the CSV files into DataFrames
        df1 = pd.read_csv(holdover,dtype={join_column: 'Int64'})
        df2 = pd.read_csv(filename,dtype={join_column: 'Int64'})


        requests_processed = set()
        rows_to_remove = set()
        for index, row in df2.iterrows():
            if row['Status'] == 'TRIGGER_SKIPPED':
                rows_to_remove.add(index)
            else:
                requests_processed.add(row['Workflow Execution Id'])

        for index, row in df2.iterrows():
            if row['Workflow Execution Id'] not in requests_processed:
                requests_processed.add(row['Workflow Execution Id'])
                rows_to_remove.remove(index)

        df2 = df2.drop(index=rows_to_remove).reset_index(drop=True)
        df2 = df2.drop_duplicates(subset='Load Number', keep='first')

        # Perform the join on the specified column
        result = pd.merge(df1, df2, on=join_column, how=how)
        result.to_csv(f'{self.shipper_id}_holdover_JOIN_{self.workflow_identifier}.csv', index=False)
        return f'{self.shipper_id}_holdover_JOIN_{self.workflow_identifier}.csv'
        

    # Load the CSV file
    df = pd.read_csv(filename)

    # Update the 'NOTES/COMMENTS' column based on conditions
    df['NOTES/COMMENTS'] = df.apply(
        lambda row: 'Email sent and awaiting response' if row['Workflow'] == 'ready_to_pickup' and pd.isnull(row['Update Actions'])
        else 'Email sent and response processed' if row['Workflow'] == 'ready_to_pickup' and '_UPDATED' in row['Update Actions'].upper()
        else 'Email sent and no response processed' if row['Workflow'] == 'ready_to_pickup'
        else 'Email not sent',
        axis=1
    )

    # Save the updated DataFrame back to CSV
    df.to_csv(filename, index=False)
    print(f'CSV file updated successfully to: {filename}')

    def process_logs_and_update_report(self,log_csv_path, report_csv_path, output_csv_path):
        # Step 1: Read log CSV and create load_map
        log_df = pd.read_csv(log_csv_path)
        log_df = log_df[['load_number', 'date']]
        log_df.to_csv(log_csv_path, index=False)

        load_map = {}
        with open(log_csv_path, mode='r') as log_file:
            log_reader = csv.reader(log_file)
            next(log_reader)  # Skip header row
            for row in log_reader:
                try:
                    load_number = row[0]
                    load_number = int(load_number)
                    # Explicitly add the current year to avoid ambiguity
                    date_str = f"{row[1]}-{datetime.now().year}"
                    date = datetime.strptime(date_str, '%d-%b-%Y')
                    formatted_date = date.strftime("%Y-%m-%d")
                    if load_number not in load_map:
                        load_map[load_number] = []
                    load_map[load_number].append(formatted_date)
                except ValueError as e:
                    print(f"Skipping invalid row in log CSV: {row}, Error: {e}")
                    continue
            print('the load map: ',load_map)
    
        # Step 2: Read the final report and process rows
        updated_rows = []
        with open(report_csv_path, mode='r') as report_file:
            report_reader = csv.DictReader(report_file)
            fieldnames = report_reader.fieldnames
            for row in report_reader:
                try:
                    if row['NOTES/COMMENTS'] == 'Email not sent':
                        print('Inside')
                        load_number = row['Load Number']
                        load_number = int(float(load_number))
                        print('The Load_number: ',load_number)
                        # Parse the date
                        def parse_date(date_str):
                                date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                return date_obj.strftime('%Y-%m-%d')
                            
                        if row.get('Triggered At'):
                            start_date = parse_date(row['Triggered At'])
                            # Add the current year
                            # start_date = start_date.replace(year=datetime.now().year)

                            # Format the date as 'YYYY-MM-DD'
                            # start_date = start_date.strftime('%Y-%m-%d')
                            print('The Start_date: ',start_date)
                            if load_number in load_map:
                                print('The load matched')
                                for log_date in load_map[load_number]:
                                    print('The Log_date: ',log_date)
                                    print('The Start_date: ',start_date)
                                    if log_date >= start_date:
                                        row['NOTES/COMMENTS'] = 'Email Skipped as Expected'
                                        break
                except ValueError as e:
                    print(f"Skipping invalid row in report CSV: {row}, Error: {e}")
                    continue
                updated_rows.append(row)
        
        # Step 3: Write the updated rows to a new CSV
        with open(output_csv_path, mode='w', newline='') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updated_rows)



    def run(self):
        """Fetch, process, and save the data."""
        # Fetch data from both databases
        query_1 = get_agentic_audit_logs_query(self.workflow_identifier,self.shipper_id, self.start_date, self.end_date)
        df_db1 = self.fetch_data(self.db1_url, query_1)

        query_2 = get_milestones_query(self.shipper_id, self.workflow_identifier, self.start_date, self.end_date)
        df_db2 = self.fetch_data(self.db2_url, query_2)

        # here self.df is the merged data of df1 and df2
        self.process_data(df_db1, df_db2)
        # save to file: 
        filename = self.save_to_csv()

        carrier_updater = CarrierUpdater(filename)

        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]

        #load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
        self.df = carrier_updater.update_carrier_info()

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)
        self.add_response_time(filename)
        output_file = self.join(self.holdover,filename)
        self.fill(output_file)
        # self.process_logs_and_update_report(self.log_csv_path,output_file,self.output_csv_path)


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
    workflow_identifier = 'ready_to_pickup'
    holdover = 'smithfield-holdover-report-31stOCT-20thNov.csv'
    log_csv_path = 'email_skipped_merged.csv'
    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    year = date_obj.year
    output_csv_path = f'rtp_report_{convert_date_to_custom_format(start_date)}_{convert_date_to_custom_format(end_date)}{year}.csv'
    main_process = Main(shipper_id, start_date, end_date,workflow_identifier,holdover,log_csv_path,output_csv_path)
    main_process.run()
