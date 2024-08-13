# %%
import requests
from dotenv import load_dotenv
import os
import json
import pandas as pd

from _globals import PRODUCT_COLUMN_MAPPING

load_dotenv()

auth_details = {
    "url": os.getenv("APEX_ORACLE_DB_URL"),
    "client_id": os.getenv("APEX_ORACLE_CLIENT_ID"),
    "client_password": os.getenv("APEX_ORACLE_CLIENT_SECRET"),
}

endpoints = {
    "insert_product": {
        "url": "/products/{product_id}",
        "method": "GET",
        "headers": {"Authorization": "Bearer {access_token}"},
    },
    "insert_products": {
        "url": "/products/insert_staging",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json",
        },
        "data": "data_to_insert.json",
    },
    "batch_load_products": {
        "url": "/items_staging/batchload",
        "method": "POST",
        "headers": {
            "Content-Type": "text/csv",
            "Authorization": "Bearer {access_token}",
        },
        "data": "data_to_insert.csv",
    },
}


class OracleAuthentication:
    def __init__(self, url: str, client_id: str, client_password: str):
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


def query_product(product_id: str, auth: OracleAuthentication):
    query_url = f"{auth.base_url}/products/{product_id}"
    data = requests.get(
        query_url.format(product_id=product_id),
        headers={"Authorization": f"Bearer {auth.access_token}"},
    )
    return data.json()


def insert_product(auth: OracleAuthentication, data_to_insert: dict):
    insert_url = f"{auth.base_url}/products/insert_staging"
    data = requests.post(
        insert_url,
        headers={"Authorization": f"Bearer {auth.access_token}", **data_to_insert},
    )
    return data


def get_all_products(auth: OracleAuthentication, product_id: int = None):
    product_id = "" if product_id is None else product_id
    query_url = f"{auth.base_url}/items_staging/{product_id}"
    data = requests.get(
        query_url,
        headers={"Authorization": f"Bearer {auth.access_token}"},
    )
    return data.json()


def batch_load_products(auth: OracleAuthentication, csv_file_path: str):
    insert_url = f"{auth.base_url}/items_staging/batchload"
    with open(csv_file_path, "rb") as csv_file:
        data = csv_file.read()
        response = requests.post(
            insert_url,
            headers={
                "Authorization": f"Bearer {auth.access_token}",
                "Content-Type": "text/csv",
            },
            data=data,
        )
    return response


# TODO: need to customize the procedure in oracle to return error messages/response
def load_items(auth: OracleAuthentication):
    query_url = f"{auth.base_url}/products/load_items"
    response = requests.post(
        query_url,
        headers={"Authorization": f"Bearer {auth.access_token}"},
    )
    return response


def parse_load_items_error(response):
    result = json.loads(response.json()["result"])
    return result


def check_load_items_status(result):
    return result["status"] == "success"  # return True if success else False


# use pydantic to validate csv or json data to make sure column names and data types are correct

# %%
csv_data = "product_mock_for_apex.csv"
formatted_csv_data = "product_mock_for_apex_formatted.csv"
df = pd.read_csv(csv_data)
df = df.rename(columns=PRODUCT_COLUMN_MAPPING)
df = df.drop(columns=["ZITA_CATEGORY"])
df = df.fillna("")
df["ID"] = ""  # ID is not a nullable column in DB so must be in the csv even if blank
df.to_csv(
    formatted_csv_data, index=False, encoding="utf-8"
)  # only encoding utf-8 is supported

response = batch_load_products(auth=auth, csv_file_path=formatted_csv_data)
print(response.text)

# %%
load_execution_response = load_items(auth)
result = parse_load_items_error(load_execution_response)
print(check_load_items_status(result))
# %%
