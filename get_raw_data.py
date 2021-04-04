from __future__ import print_function
import httplib2
import os, io
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from apiclient.http import MediaIoBaseDownload
import auth
import requests
import pdfx
from time import time
from datetime import datetime

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'
authInst = auth.auth(SCOPES,CLIENT_SECRET_FILE,APPLICATION_NAME)
credentials = authInst.getCredentials()
http = credentials.authorize(httplib2.Http())
drive_service = discovery.build('drive', 'v3', http=http)
start_timer = time()

def get_credentials():
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: 
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    dir_name = '/Users/bernadettechia/Downloads/covidtracker/raw_data'
    for item in os.listdir(dir_name):
        if item.endswith('.csv'):
            os.remove(os.path.join(dir_name, item))
            print("Deleted yesterday's csv file!")

    return_folder_id('http://bit.ly/DataDropPH')


def return_folder_id(short_url): # expand bitly URLs
    folder_url = str(requests.head(short_url).headers['location']) 
    folder_id = folder_url[39:72]
    get_files(folder_id)
    

def get_files(folder_id):
    files_metadata = get_folder_contents(folder_id)

    if len(files_metadata) == 1: # dl the pdf containing the URL to actual files
        files_id = files_metadata[0]['id']
        actually_download_file(files_id, 'readme.pdf') 
        pdf = pdfx.PDFx('readme.pdf')  
        links_list = pdf.get_references_as_dict()['url'] 
        csvs_link = [i for i in links_list if i.startswith('https://bit.ly/')]
        print("Today's source files are stored in: " + csvs_link[0])
        return_folder_id(csvs_link[0])

    else: # dl desired csv's
        wanted_list = ['Case Information', 'DOH Data Collect - Daily Report', 'Testing Aggregates']
        for item in files_metadata:
            for wanted in wanted_list:
                idx = files_metadata.index(item)
                filename = files_metadata[idx]['name']
                if wanted in filename:
                    new_name = datetime.strptime(filename[21:29],'%Y%m%d').strftime('%Y-%m-%d_') + wanted + '.csv'
                    actually_download_file(files_metadata[idx]['id'], '%s' %new_name)
        print('Your raw data download only took {0:0.1f} seconds. See you tomorrow!'.format(time() - start_timer))

    

def actually_download_file(file_id,filepath):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Downloaded %s %d%%." %(filepath, int(status.progress() * 100)))
    with io.open(filepath,'wb') as f:
        fh.seek(0)
        f.write(fh.read())


def get_folder_contents(folder_id):
    results = drive_service.files().list(
        q=("'{0}' in parents".format(folder_id)),
        corpora="user",
        fields="nextPageToken, files(id, name, webContentLink, " +
               "createdTime, modifiedTime)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    return items


if __name__ == '__main__':
    main()