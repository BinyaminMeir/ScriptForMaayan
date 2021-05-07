#!/usr/bin/env python3
# import the required libraries
import csv
import io
import pickle
import os.path
import requests
import sys

from collections import namedtuple
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload


################## CONSTS ##################
#The file's name in GDrive.
DATASET_FILE_NAME = "test_file"

#The spreadsheet's name.
SAMPLE_SPREADSHEET_ID = "test_spreadsheet"
# The range in the spreadsheet.
# E.g. for a table A2 to E7, in the spreadsheet named "Class Data" -  'Class Data!A2:E7'
SAMPLE_RANGE_NAME = 'Class Data!A2:E7'
############## END OF CONSTS ###############

DataTypeHandler = namedtuple('DataTypeHandler', ['scopes', 'pseudo_file' ,'data_extract_func'])

DATA_SOURCES = { "DRIVE" :  DataTypeHandler(scopes=['https://www.googleapis.com/auth/drive.file'], 
                                            get_data = __get_dataset_data),
                 "SPREADSHEETS" : DataTypeHandler(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],  
                                            get_data = __get_spreadsheet_data),
                }

def get_creds(scope):
    # Variable creds will store the user access token.
    # If no valid token found we will create one.
    creds = None

    # The file token.pickle stores the user's access and refresh tokens.
    #  It is created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.pickle'):
        # Read the token from the file and store it in the variable creds
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If no valid credentials are available, request the user to log in.
    if not creds or not creds.valid:
        # If token is expired, it will be refreshed, else, we will request a new one.
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scope)
            creds = flow.run_local_server(port=0)

        # Save the access token in token.pickle file for future usage
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds

def __get_dataset_data(creds):
     # Connect to the API service
    service = build('drive', 'v3', credentials=creds)

    # request the file id from the API.
    results = service.files().list(q = f"name = '{DATASET_FILE_NAME}'", fields="files(id, name)").execute()
    items = results.get('files', [])

    if len(items) == 0:
        raise NameError(f"Could not find file with the name <{DATASET_FILE_NAME}> on the drive.")

    if len(items) > 1:
        raise NameError(f"More than one file with the name <{DATASET_FILE_NAME}> found on the drive.")

    request = service.files().get_media(fileId=items[0].get('id'))
    fh = io.BytesIO()

    # Initialise a downloader object to download the file
    downloader = MediaIoBaseDownload(fh, request, chunksize=204800)
    done = False

    try:
        # Download the data in chunks:
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)

        raw_data = list(csv.reader(fh))
        data = raw_data[1:] #Droping the header.
        return data
        
    except:
        #TODO: Maybe we should handle some exceptions?
        raise

def __get_spreadsheet_data(creds):    
    """
    Returns matrix of values. 
    """
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])
    return values




def magic(type):
    data_type = DATA_SOURCES[type.upper()]
    creds = get_creds(data_type.scopes)
    dataset = data_type.get_data(creds)


if __name__ == "__main__":
	magic(sys.argv[1])
