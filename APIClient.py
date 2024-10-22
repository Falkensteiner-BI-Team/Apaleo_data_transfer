import requests

import json
import userdata

def get_token():
    token_url = 'https://identity.apaleo.com/connect/token'
    # Define your client ID and client secret
    client_id = userdata.client_id()
    client_secret = userdata.client_secret()

    # data to be sent for token request
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(token_url, data=token_data)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        return access_token
    else:
        print(f"Failed to obtain access token. Status code: {response.status_code}")


class APIClient:
    def __init__(self, base_url, access_token):

        self.base_url = base_url
        self.access_token = access_token

        self.headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

    def get_data(self):
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            # Request was successful
            return response.json()

    def post_data(self, property_id, date_filters):

        params = {
            "propertyId": property_id,
            "dateFilter": date_filters

        }

        response = requests.post(self.base_url, headers=self.headers, params = params)
        return response.json()

    def post_data_(self):



        response = requests.post(self.base_url, headers=self.headers)
        return response.json()

    def extract_to_json(self,data,filepath):
        with open(filepath, 'a', encoding='utf-8') as File:
            json.dump(data, File, ensure_ascii=False)
            File.write('\n')

    def read_json(self,file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data
