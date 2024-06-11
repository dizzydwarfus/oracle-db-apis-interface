import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

auth_details = {
    "url": os.getenv("APEX_ORACLE_DB_URL"),
    "client_id": os.getenv("APEX_ORACLE_CLIENT_ID"),
    "client_password": os.getenv("APEX_ORACLE_CLIENT_SECRET"),
}


class OracleAuthentication:
    def __init__(self, url, client_id, client_password):
        self.client_id = client_id
        self.client_password = client_password
        self.base_url = url
        self.token_url = f"{self.base_url}/oauth/token"
        self.response = None
        self.access_token = None

    def authenticate(self):
        data = {"grant_type": "client_credentials"}
        auth_data = (self.client_id, self.client_password)
        self.response = requests.post(self.token_url, data=data, auth=auth_data)
        self.access_token = self.response.json()["access_token"]


auth = OracleAuthentication(**auth_details)
auth.authenticate()


def query_product(product_id, auth):
    query_url = f"{auth.base_url}/products/{product_id}"
    data = requests.get(
        query_url.format(product_id=product_id),
        headers={"Authorization": f"Bearer {auth.access_token}"},
    )
    return data.json()


def insert_product(auth, data_to_insert):
    insert_url = f"{auth.base_url}/products/insert_staging"
    data = requests.post(
        insert_url,
        headers={"Authorization": f"Bearer {auth.access_token}", **data_to_insert},
    )
    return data.json()


with open("data_to_insert.json", "r") as file:
    data_to_insert = json.loads(file.read())

# insert_product(auth, data_to_insert)
