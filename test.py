# %%
# Built-in Imports
import os

# Third Party Imports
from dotenv import load_dotenv
import pandas as pd

# Local Imports
from _globals import PRODUCT_COLUMN_MAPPING, ENDPOINTS
from utils.auth import OracleAuthentication
from OracleAPI import OracleAPI

load_dotenv()

auth_details = {
    "url": os.getenv("APEX_ORACLE_DB_URL"),
    "client_id": os.getenv("APEX_ORACLE_CLIENT_ID"),
    "client_password": os.getenv("APEX_ORACLE_CLIENT_SECRET"),
}

auth = OracleAuthentication(**auth_details)
auth.authenticate()

oracle_api = OracleAPI(auth, ENDPOINTS)

# use pydantic to validate csv or json data to make sure column names and data types are correct

# %% Process data to be loaded
csv_data = os.path.join("mock_data", "product_mock_for_apex.csv")
formatted_csv_data = os.path.join("mock_data", "product_mock_for_apex_formatted.csv")
df = pd.read_csv(csv_data)
df = df.rename(columns=PRODUCT_COLUMN_MAPPING)
df = df.drop(columns=["ZITA_CATEGORY"])
df = df.fillna("")
df["ID"] = ""  # ID is not a nullable column in DB so must be in the csv even if blank
df.to_csv(
    formatted_csv_data, index=False, encoding="utf-8"
)  # only encoding utf-8 is supported

response = oracle_api.batch_load(
    load_object="products",
    csv_file_path=formatted_csv_data,
)
print(response.text)

# %%
load_execution_response = oracle_api.execute_load(load_object="products")
# %%
