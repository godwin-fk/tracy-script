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
            'scac' : 'SCAC',
            'workflow': 'Workflow',
            'goal': 'Goal',
            'raw_message': 'Raw Response',
            'alert': 'Alert',
            'start_time': 'Start Time',
            'end_time': 'End Time',
            'actions': 'Updated data points on FK',
            'reminder': 'Reminder',
            'escalated': 'Escalated'
        })
        # Rearrange the columns in the desired order
        self.df = self.df[['Load Number','Request ID', 'Shipper', 'Carrier','SCAC', 'Workflow','Goal','Raw Response', 'Alert','Start Time','End Time','Updated data points on FK','Reminder', 'Escalated']]
        # TODO: sort by start_time and load_id , use both fields to sort
        self.df['Start Time'] = pd.to_datetime(self.df['Start Time'])
        self.df=self.df.sort_values(by=['Start Time', 'Load Number'])
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
        # TODO: merge over source ID instead of load_id
        merged_df= pd.merge(df_db1, df_db2, on=['load_id'], how='inner')

        merged_df = merged_df.drop(columns=["shipper_id_y"]).rename(columns={"shipper_id_x": "shipper_id"})
        
        # Drop duplicates if any
        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.fillna('')
        self.df = merged_df
    

    def join(self,holdover,filename,join_column='Load Number', how='left'):
        # Load the CSV files into DataFrames
        df1 = pd.read_csv(holdover,dtype={join_column: 'Int64'})
        df2 = pd.read_csv(filename,dtype={join_column: 'Int64'})
        df2 = df2.drop_duplicates(subset='Load Number', keep='first')

        # Perform the join on the specified column
        result = pd.merge(df1, df2, on=join_column, how=how)
        result.to_csv(f'{self.shipper_id}_holdover_JOIN_{self.workflow_identifier}.csv', index=False)
        return f'{self.shipper_id}_holdover_JOIN_{self.workflow_identifier}.csv'
        

    def fill(self,filename):
        # Load the CSV file
        
        df = pd.read_csv(filename)

        df['NOTES/COMMENTS'] = df.apply(
            lambda row: (
                'Email sent and no response processed'
                if row['Workflow'] == 'ready_to_pickup' and pd.isnull(row['Updated data points on FK'])
                else 'Email sent and response processed'
                if row['Workflow'] == 'ready_to_pickup'
                else 'Email not sent'
            ),
            axis=1
        )
        # Save the updated DataFrame back to CSV
        df.to_csv(filename, index=False)
        print("CSV file updated successfully.")

    def process_logs_and_update_report(self,log_csv_path, report_csv_path, output_csv_path):
        # Step 1: Read log CSV and create load_map
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
                                try:
                                    # Attempt to parse 'Nov-05' (month-day)
                                    return datetime.strptime(date_str, '%b-%d')
                                except ValueError:
                                    # If the above fails, try parsing '7-Nov' (day-month)
                                    return datetime.strptime(date_str, '%d-%b')
                                                        
                        start_date = parse_date(row['DATE'])
                        # Add the current year
                        start_date = start_date.replace(year=datetime.now().year)

                        # Format the date as 'YYYY-MM-DD'
                        start_date = start_date.strftime('%Y-%m-%d')
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
        query_2 = get_milestones_query(self.shipper_id, self.start_date, self.end_date)
        df_db1 = self.fetch_data(self.db1_url, query_1)
        df_db2 = self.fetch_data(self.db2_url, query_2)
        # here self.df is the merged data of df1 and df2
        self.process_data(df_db1, df_db2)
        # save to file: 
        filename = self.save_to_csv()

        carrier_updater = CarrierUpdater(filename)

        shipment_numbers = self.df['load_id'].tolist()
        shipment_numbers = [str(x) for x in shipment_numbers]

        load_responses = carrier_updater.search_shipments_with_pagination(shipment_numbers, shipper_id, 'graph_id')
        self.df = carrier_updater.update_carrier_info(load_responses)

        # Format and Save the updated DataFrame to a CSV file
        self.format_and_save_df(filename)
        output_file = self.join(self.holdover,filename)
        self.fill(output_file)
        self.add_response_time(output_file)
        self.process_logs_and_update_report(self.log_csv_path,output_file,self.output_csv_path)


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
    end_date = '2024-11-06'
    workflow_identifier = 'ready_to_pickup'
    holdover = 'Smithfield_holdover_31-7-1.csv'
    log_csv_path = 'email_skipped_logs.csv'
    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    year = date_obj.year
    output_csv_path = f'rtp_report_{convert_date_to_custom_format(start_date)}_{convert_date_to_custom_format(end_date)}{year}.csv'
    main_process = Main(shipper_id, start_date, end_date,workflow_identifier,holdover,log_csv_path,output_csv_path)
    main_process.run()
