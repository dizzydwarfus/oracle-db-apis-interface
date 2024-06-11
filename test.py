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

# product_id = 3526
# query_url = f"{auth.base_url}/products/{product_id}"

# data = requests.get(
#     query_url.format(product_id=product_id),
#     headers={"Authorization": f"Bearer {auth.access_token}"},
# )
# print(json.dumps(data.json(), indent=2))

# Insert into staging
data_to_insert = {
    "article_number": "BART/000000000050676869",
    "corresponding_article_number": "ABS-0107b075",
    "basf_bi_number": "993285",
    "cobalt_number": "50676869",
    "product_description": "Ultrafuse® ABS Green 2.85mm 750G 4G",
    "isactive": "Y",
    "quantity_per_unit": "1",
    "packaging_size_per_unit": "0.75",
    "packaging_size_uom": "kg",
    "status": "Commercial",
    "salesforce_number": "01t6N000002mYQAQA2",
    "global_item_code": "8100010",
    "business_line": "Additive Extrusion Solutions (AES)",
    "product_subfamily": "ABS",
    "technologies": "Plastic Filament",
    "controlling_group": "AES Standard",
    "brand": "Ultrafuse®",
    "color": "Green",
    "source_system": "Cobalt",
    "product_region": "EMEA",
    "production_location_status": "Central",
    "diameter": "2.85",
    "quantity_uom": "Spool",
}

insert_url = f"{auth.base_url}/products/insert_staging"

data = requests.post(
    insert_url,
    headers={"Authorization": f"Bearer {auth.access_token}", **data_to_insert},
)

load_items_url = f"{auth.base_url}/products/load_items"
requests.post(
    load_items_url,
    headers={"Authorization": f"Bearer {auth.access_token}"},
)
