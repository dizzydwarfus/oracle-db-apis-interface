from typing import Literal
import json
import requests

from utils.auth import OracleAuthentication


class OracleAPI:
    def __init__(self, auth: OracleAuthentication, endpoints: dict):
        self.auth = auth
        self.endpoints = endpoints
        self.errors = {
            "procedure_error": [],
            "query_error": [],
            "batch_load_error": [],
        }

    def query(
        self, query_object: Literal["products", "customers", "facts"], query_id=None
    ):
        query_id = "" if query_id is None else query_id
        query_url = (
            f"{self.auth.base_url}/{self.endpoints['get_query'][query_object]['url']}"
        )
        response = requests.get(
            query_url.format(query_id=query_id),
            headers={
                "Authorization": f"Bearer {self.auth.access_token}",
            },
        )
        return response.json()

    def batch_load(
        self, load_object: Literal["products", "customers", "facts"], csv_file_path: str
    ):
        insert_url = (
            f"{self.auth.base_url}{self.endpoints['batch_load'][load_object]['url']}"
        )
        with open(csv_file_path, "rb") as csv_file:
            data = csv_file.read()
            response = requests.post(
                insert_url,
                headers={
                    "Authorization": f"Bearer {self.auth.access_token}",
                    "Content-Type": self.endpoints["batch_load"][load_object][
                        "headers"
                    ]["Content-Type"],
                },
                data=data,
            )
        return response

    def execute_load(self, load_object: Literal["products", "customers", "facts"]):
        insert_url = (
            f"{self.auth.base_url}{self.endpoints['execute_load'][load_object]['url']}"
        )
        response = requests.post(
            insert_url,
            headers={
                "Authorization": f"Bearer {self.auth.access_token}",
            },
        )
        return response

    def check_load_error(self, response):
        """Check if the PL/SQL procedure for loading rows from staging table into production table returned an error

        Args:
            response (HTTP Reponse): pass in the response object from the execute_load methods

        Returns:
            bool: Returns True if the load was successful, else False
        """
        result = json.loads(response.json()["result"])

        self.errors["procedure_error"].extend(result["errors"]) if result[
            "status"
        ] == "error" else None
        return result["status"] == "success"
