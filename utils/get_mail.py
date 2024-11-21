import imaplib
import email
from email.header import decode_header
import pandas as pd 

# Gmail IMAP server and port
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

# Your credentials
EMAIL = "mail-id"
APP_PASSWORD = "password"  # Use the generated app password

import os
import datetime
from email.utils import parsedate_to_datetime

def read_email_by_id(email_id):
    try:
        # Connect to the Gmail server
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        
        # Login to your account
        mail.login(EMAIL, APP_PASSWORD)
        
        # Select the mailbox you want to read (e.g., "INBOX")
        mail.select("inbox")
        
        # Fetch the email by its ID (use the email_id provided)
        status, data = mail.fetch('CO6PR04MB83649A9A32BFC0DBC1BD6926C65F2@CO6PR04MB8364.namprd04.prod.outlook.com', "(RFC822)")

        # status, data = mail.fetch(f'"{email_id}"', "(RFC822)")
        
        if status == "OK":
            # Parse the email content
            msg = email.message_from_bytes(data[0][1])
            
            # Decode the email subject
            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                subject = subject.decode(encoding or "utf-8")
            
            # Get the sender’s information
            from_ = msg.get("From")
            
            # Parse the email date
            email_date = msg.get("Date")
            email_datetime = parsedate_to_datetime(email_date)
            
            # Define the date range for filtering
            start_date = datetime.datetime(2024, 11, 7)
            end_date = datetime.datetime(2024, 11, 20)
            
            # Criteria check: Date range, subject filtering
            if (start_date <= email_datetime <= end_date and 
                "holdover report" in subject.lower() and 
                "holdover report template" not in subject.lower()):
                
                # Print email details
                print(f"Processing Email - Subject: {subject}, From: {from_}, Date: {email_date}")
                
                # Create the attachment folder if it doesn't exist
                attachment_dir = "attachments"
                os.makedirs(attachment_dir, exist_ok=True)
                
                # Check if the email message is multipart
                if msg.is_multipart():
                    for part in msg.walk():
                        content_disposition = str(part.get("Content-Disposition"))
                        
                        # Check for attachments
                        if "attachment" in content_disposition:
                            filename = part.get_filename()
                            if filename:
                                filepath = os.path.join(attachment_dir, filename)
                                
                                # Save the attachment
                                with open(filepath, "wb") as f:
                                    f.write(part.get_payload(decode=True))
                                print(f"Attachment saved: {filepath}")
                else:
                    print("No attachments found.")
            else:
                print(f"Email does not meet the criteria - Subject: {subject}, Date: {email_date}")

        else:
            print(f"Failed to fetch the email with ID {email_id}.")

        # Logout after processing the email
        mail.logout()

    except Exception as e:
        print(f"An error occurred while processing email ID {email_id}: {e}")




# Function to process the CSV file and call read_email_by_id for each email ID
def process_csv_and_read_emails(csv_file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        # Ensure the column 'entity_id' exists
        if 'entity_id' not in df.columns:
            print("The CSV file does not have a column named 'entity_id'.")
            return
        
        # Iterate through the 'entity_id' column
        for email_id in df['entity_id']:
            # Convert email ID to string and strip any extra spaces
            email_id = str(email_id).strip()
            
            # Call the function to read the email
            print(f"\nProcessing email ID: {email_id}")
            read_email_by_id(email_id)
    
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")

# Replace 'emails.csv' with the path to your CSV file
csv_file_path = 'rtp-emails-7Nov-20Nov2024.csv'
process_csv_and_read_emails(csv_file_path)

