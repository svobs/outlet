#!/usr/bin/python3
from __future__ import print_function
import pickle
import os.path
import base64
import email
from apiclient import errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    ####

    try:
        user_id = 'me'
        query='from:USAA.Customer.Service@mailcenter.usaa.com (Subscribed Alert)'
        label_ids=[]

        # Get Messages (query):
        response = service.users().messages().list(userId=user_id, q=query).execute()
        # Get Messages (labels):
        #response = service.users().messages().list(userId=user_id, labelIds=label_ids).execute()
        # Some known labelIds = ['UNREAD', 'IMPORTANT', 'CATEGORY_UPDATES', 'INBOX']
        if not response:
            print('No messages found!')
        else:
            messages = []

            # Note: returned list contains Message IDs, you must use get with the appropriate id to get the details of a Message.
            if 'messages' in response:
                messages.extend(response['messages'])
                print("ResultSizeEstimate: " + str(response['resultSizeEstimate']))

                for msg in response['messages']:
                    # Note: must use format='raw' to get body; using 'full' does not populate payload.body as in the spec
                    message = service.users().messages().get(userId=user_id, id=msg['id'], format='raw').execute()
                    print('Message snippet: %s' % message['snippet'])
                    msg_bytes = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
                    mime_msg = email.message_from_string(msg_bytes.decode('ASCII'))
                #    payload = message['payload']
                #    body = payload['body']

                    print(" XXX BEGIN MIME MSG XXX")
                    print(mime_msg)
                #    for field in message:
                #        print(f'{field} = {message[field]}')
                    print(" XXX END MIME MSG XXX")

            while 'nextPageToken' in response:
                print('Getting next token')
                page_token = response['nextPageToken']
                response = service.users().messages().list(userId=user_id, q=query, pageToken=page_token).execute()

                messages.extend(response['messages'])
                print("ResultSizeEstimate:: " + str(response['resultSizeEstimate']))

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

