import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta

def test_email_filtering(username, password, start_date, end_date):
    """
    Test email filtering by printing subjects and dates of emails within a specified date range
    and filtering for specific subject conditions.
    
    :param username: Gmail username (email address)
    :param password: Gmail password or app-specific password
    :param start_date: Start date for email search (format: 'DD-MMM-YYYY')
    :param end_date: End date for email search (format: 'DD-MMM-YYYY')
    """
    # Connect to Gmail's IMAP server
    imap_server = "imap.gmail.com"
    imap_port = 993
    
    # Connect to the server
    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    
    try:
        # Login to the account
        mail.login(username, password)
        
        # Select the mailbox you want to access (default is INBOX)
        mail.select('INBOX')
        
        # Convert dates to required format for IMAP search
        start = datetime.strptime(start_date, '%d-%b-%Y')
        end = datetime.strptime(end_date, '%d-%b-%Y')
        
        # IMAP search criteria for date range
        search_criteria = f'(SINCE "{start.strftime("%d-%b-%Y")}" BEFORE "{(end + timedelta(days=1)).strftime("%d-%b-%Y")}" OR (SUBJECT "Hold") (SUBJECT "Holdover"))'
        
        # Search for emails within the date range
        status, messages = mail.search(None, search_criteria)
        
        if status != "OK" or not messages[0]:
            print("No emails found in the specified date range.")
            return
        
        print(f"Filtered emails: #{len(messages[0].split())}")

        # Iterate through all email IDs
        for num in messages[0].split():
            # Fetch the email message by ID
            status, msg_data = mail.fetch(num, '(RFC822)')
            
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    # Parse the email
                    msg = email.message_from_bytes(response_part[1])
                    
                    # Decode email subject
                    subject = decode_header(msg["Subject"])[0][0]
                    if isinstance(subject, bytes):
                        subject = subject.decode()
                    
                    # Get email date
                    date = msg["Date"]
                    
                    # Filter emails by subject
                    if ('holdover report' in subject.lower() and 
                        'holdover report template' not in subject.lower()):
                        
                        print(f"Subject: {subject}")
                        print(f"Date: {date}")
                        print("-" * 50)
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        mail.close()
        mail.logout()

# Example usage
def main():
    # Prompt for credentials and dates
    username = 'mail-id'
    password = 'password'
    start_date = '07-Nov-2024'
    end_date = '20-Nov-2024'
    
    # Test email filtering
    test_email_filtering(username, password, start_date, end_date)

if __name__ == "__main__":
    main()
