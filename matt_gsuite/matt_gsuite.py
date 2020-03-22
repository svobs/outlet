#!/usr/bin/python3
from __future__ import print_function
import pickle
import os.path
import base64
import email
import re
import mimetypes
from apiclient import errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def process_single_msg(user_id, msg_id, service):
    # Note: must use format='raw' to get body; using 'full' does not populate payload.body as in the spec
    message = service.users().messages().get(userId=user_id, id=msg_id, format='raw').execute()
    #print('Message snippet: %s' % message['snippet'].encode('ASCII'))
    payload = message['raw']
    msg_str = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
    mime_msg = email.message_from_string(msg_str.decode('ASCII'))

    for part in mime_msg.walk():
        if part.get_content_type() == 'text/plain':
            print(" ------ START ------")
            body = part.get_payload(decode=True)
            # Convert newlines and tabs
            body_str = str(body).replace(r'\n', '\n').replace(r'\t', '\t')
            #print(body_str)
            #print("---")

            # TODO: externalize account #s
            account_p = re.compile(r'Account ending in:\s*5101', re.MULTILINE | re.DOTALL)
            m1 = account_p.search(body_str)
            if m1:
                print(m1.group(0))
                balance_p = re.compile(r'Available balance:\s*\$[\d,.]*', re.MULTILINE | re.DOTALL)
                m2 = balance_p.search(body_str)
                if m2:
                    print(m2.group(0))
            else:
                print('No match')
            print(" ------ END ------")


def load_gmail_service():
    token_file_path = 'token.pickle'
    credentials_file_path = 'credentials.json'
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_file_path):
        with open(token_file_path, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_file_path, 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service


def main():

    service = load_gmail_service()
    ####

    try:
        user_id = 'me'
        query='from:USAA.Customer.Service@mailcenter.usaa.com (Subscribed Alert)'
        # Some known labelIds = ['UNREAD', 'IMPORTANT', 'CATEGORY_UPDATES', 'INBOX']
        label_ids=['UNREAD']
        processed_msg_count = 0
        page_token = None
        while True:
            # Get Messages (query):
            # Note: returned list contains Message IDs, you must use 'get' with the appropriate id to get the details of a Message.
            response = service.users().messages().list(pageToken=page_token, userId=user_id, q=query, labelIds=label_ids).execute()
            if not response or 'messages' not in response:
                print('No messages found!')
            else:
                messages = []

            print("ResultSizeEstimate: " + str(response['resultSizeEstimate']))

            for msg in response['messages']:
                process_single_msg(user_id, msg['id'], service)
                processed_msg_count = processed_msg_count + 1

            if 'nextPageToken' in response:
                page_token = response['nextPageToken']
            else:
                break

        print("Total messages processed: " + str(processed_msg_count))

    except errors.HttpError as error:
        print('An error occurred: %s', error)

"""
    # Get Profile
    response = service.users().getProfile(userId='me').execute()
    if not response:
        print('No profile found.')
    else:
        print('Profile:')
        for field in response:
            print(f'{field} = {response[field]}')
"""

"""

    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])

    if not labels:
        print('No labels found.')
    else:
        print('Labels:')
        for label in labels:
            print(label['name'])

"""

if __name__ == '__main__':
    main()

