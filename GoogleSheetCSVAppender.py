import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"
CSV_FILE_PATH = "YOUR_CSV_PATH"
TOKEN_JSON_PATH = "token.json"
CREDENTIALS_JSON_PATH = "credentials.json"

def get_credentials():
    credentials = None
    
    # Check if token.json exists and load credentials
    if os.path.exists(TOKEN_JSON_PATH):
        try:
            credentials = Credentials.from_authorized_user_file(TOKEN_JSON_PATH, SCOPES)
        except Exception as e:
            logger.error(f"Error loading credentials from token.json: {e}")
        
    # If credentials are not valid or do not exist, refresh or authenticate
    if not credentials or not credentials.valid:
        credentials = refresh_or_authenticate_credentials()
        if credentials is None:
            return None
        
        # Save the credentials
        save_credentials(credentials)
    
    return credentials

def refresh_or_authenticate_credentials():
    try:
        if os.path.exists(TOKEN_JSON_PATH):
            credentials = Credentials.from_authorized_user_file(TOKEN_JSON_PATH, SCOPES)
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            return credentials
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_JSON_PATH, SCOPES)
            return flow.run_local_server(port=0)
    except Exception as e:
        logger.error(f"Error occurred during authentication: {e}")
        return None

def save_credentials(credentials):
    try:
        with open(TOKEN_JSON_PATH, "w") as token:
            token.write(credentials.to_json())
    except Exception as e:
        logger.error(f"Error saving credentials: {e}")

def read_csv_file(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found at {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return None

def append_data_to_sheet(service, data, spreadsheet_id, sheet_name="Sheet1"):
    try:
        # Fetch the existing data to find the last row
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:A").execute()
        values = result.get("values", [])
        last_row = len(values) if values else 0

        # Define the range to append the data
        range_to_append = f"{sheet_name}!A{last_row + 1}"

        # Prepare the value range body
        body = {
            "values": data
        }

        # Append the data
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_to_append,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()

        logging.info("Data appended successfully.")
    except HttpError as error:
        logger.error(f"An HTTP error occurred: {error}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def main():
    try:
        # Get or refresh credentials
        credentials = get_credentials()
        if not credentials:
            logger.error("Failed to obtain valid credentials.")
            return
        
        # Build the Sheets service
        service = build("sheets", "v4", credentials=credentials)

        # Read the CSV file into a DataFrame
        df = read_csv_file(CSV_FILE_PATH)
        if df is None:
            return
        
        # Convert DataFrame to list of lists
        data = df.values.tolist()

        # Append the data to the Google Sheet
        append_data_to_sheet(service, data, SPREADSHEET_ID)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
