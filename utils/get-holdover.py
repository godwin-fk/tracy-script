import imaplib
import email
import os
from datetime import datetime
from datetime import timedelta
from openpyxl import load_workbook, Workbook
import pandas as pd



def get_unique_filepath(filepath):
    base, extension = os.path.splitext(filepath)
    counter = 1
    while os.path.exists(filepath):
        filepath = f"{base}_{counter}{extension}"
        counter += 1
    return filepath

# Function to login and fetch emails
def fetch_holdover_reports(email_address, password, start_date, end_date, save_path):
    # Ensure save_path exists
    os.makedirs(save_path, exist_ok=True)

    # Connect to Gmail server
    mail = imaplib.IMAP4_SSL("imap.gmail.com")

    try:
        # Login to the account
        mail.login(email_address, password)

        # Select the inbox
        mail.select("inbox")

        # Format date ranges for IMAP search
        start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d-%b-%Y")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        adjusted_end_date = (end_date_obj + timedelta(days=1)).strftime("%d-%b-%Y")

        # Search for emails with the specified subject within the date range
        status, email_ids = mail.search(
            None,
            f'(SUBJECT "holdover" SINCE {start_date_formatted} BEFORE {adjusted_end_date})'
        )

        # Ensure search was successful
        if status != "OK":
            print("No emails found.")
            return

        # Process emails
        for email_id in email_ids[0].split():
            status, data = mail.fetch(email_id, "(RFC822)")
            if status != "OK":
                print(f"Failed to fetch email ID: {email_id}")
                continue

            # Parse the email content
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            subject = msg["subject"]

            # Ignore emails with 'holdover report template'
            if "holdover report template" in subject.lower():
                continue

            # Download attachments
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_disposition() == "attachment":
                        filename = part.get_filename()
                        if filename:
                            # filepath = os.path.join(save_path, filename)
                            filepath = get_unique_filepath(os.path.join(save_path, filename))
                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            print(f"Downloaded: {filename}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Logout from the server
        mail.logout()


def merge_xlsx_files(directory, output_file="../temp/merged_holdover_reports.xlsx"):
    """
    Merges all .xlsx files in the given directory into a single Excel file.

    :param directory: Path to the directory containing .xlsx files.
    :param output_file: Name of the output merged Excel file.
    """
    # Create a new workbook for the merged file
    merged_workbook = Workbook()
    merged_sheet = merged_workbook.active
    merged_sheet.title = "Merged Data"
    
    # Flag to track if headers have been written to the merged file
    headers_written = False

    # Iterate over all files in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith(".xlsx"):
            file_path = os.path.join(directory, file_name)
            print(f"Processing: {file_path}")
            
            # Open the workbook
            workbook = load_workbook(file_path)
            sheet = workbook.active  # Assuming data is in the first sheet

            # Read data from the current workbook
            for row_index, row in enumerate(sheet.iter_rows(values_only=True), start=1):
                # Write headers only once
                if row_index == 1 and headers_written:
                    continue
                merged_sheet.append(row)
            
            headers_written = True  # Headers have now been written
    
    # Save the merged workbook
    merged_workbook.save(output_file)
    print(f"Merged file saved as: {output_file}")
    return output_file


def clean_excel(file_path: str, output_path: str):
    """
    Cleans the Excel file by dropping rows where the first column value cannot be converted to an integer.
    
    Parameters:
    - file_path: str - Path to the input Excel file.
    - output_path: str - Path to save the cleaned Excel file.
    """
    
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Get the name of the first column
    first_col = df.columns[0]

    def is_convertible(value):
        """
        Checks if the value can be converted to an integer.
        Returns True if convertible, False otherwise.
        """
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    # Filter rows where the first column value is convertible to int
    cleaned_df = df[df[first_col].apply(is_convertible)]

    # Save the cleaned DataFrame to a new Excel file
    cleaned_df.to_excel(output_path, index=False)
    print(f"Cleaned Excel file saved to: {output_path}")



def update_headers(output_file: str):
    """
    Removes the first row of the given Excel file and adds specified headers.
    
    Parameters:
    - output_file: str - Path to the Excel file to update.
    """
    # Define the new headers
    headers = [
        "Load Number", "CARRIER", "CONTAINER ID", "DESTINATION CITY", 
        "DESTINATION STATE", "DD DATE", "DD TIME", "BILL DATE", 
        "BILL TIME", "ZDLT LATE CODE", "ON TIME? (Y/N)", "SPLIT? (Y/N)", 
        "FRESH PRIORITY STO? (Y/N)", "NOTES/COMMENTS"
    ]
    
    # Load the workbook
    wb = load_workbook(output_file)
    sheet = wb.active  # Assume we're working with the first sheet
    
    # Remove the first row
    sheet.delete_rows(1)
    
    # Insert new headers at the first row
    sheet.insert_rows(1)
    for col_index, header in enumerate(headers, start=1):
        sheet.cell(row=1, column=col_index).value = header
    
    # Save the updated workbook
    wb.save(output_file)
    print(f"Headers updated and first row removed in: {output_file}")


def convert_excel_to_csv(output_file: str, shipper_id_holdover: str, start_date: str, end_date: str):
    """
    Converts the Excel file to a CSV file and saves it with the specified filename format.
    
    Parameters:
    - output_file: str - Path to the Excel file.
    - shipper_id_holdover: str - The shipper ID for naming the CSV file.
    - start_date: str - The start date for naming the CSV file.
    - end_date: str - The end date for naming the CSV file.
    """
    # Generate the output CSV file name
    csv_filepath = f"../temp/{shipper_id_holdover}-{start_date}_{end_date}.csv"
    
    # Read the Excel file into a DataFrame
    df = pd.read_excel(output_file)
    
    # Save the DataFrame as a CSV file
    df.to_csv(csv_filepath, index=False)
    print(f"Converted Excel to CSV and saved as: {csv_filepath}")



# Input credentials and other parameters
if __name__ == "__main__":

    email_address = 'smithfield_visibility_services@fourkites.com'
    password = 'F0urKit3sR0cks'
    start_date = "2024-11-07" 
    end_date =  "2024-11-07" 
    save_path = "attachments"
    output_file = "../temp/output_data.xlsx"
    shipper_id_holdover = 'smithfield-foods-holdover'

    fetch_holdover_reports(email_address, password, start_date, end_date, save_path)
    input_file = merge_xlsx_files(save_path)
    clean_excel(input_file, output_file)
    update_headers(output_file)
    convert_excel_to_csv(output_file, shipper_id_holdover, start_date, end_date)

