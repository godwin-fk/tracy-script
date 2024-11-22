import imaplib
import email
import os
from datetime import datetime
from datetime import timedelta

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
                            filepath = os.path.join(save_path, filename)
                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            print(f"Downloaded: {filename}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Logout from the server
        mail.logout()


# Input credentials and other parameters
if __name__ == "__main__":
    email_address = "smithfield_visibility_services@fourkites.com"
    password = "F0urKit3sR0cks"
    start_date = "2024-11-07" 
    end_date =  "2024-11-20" 
    save_path = "/Users/bhanu.teja/tracy-script/utils/attachments"

    fetch_holdover_reports(email_address, password, start_date, end_date, save_path)
