from typing import Literal
import json
import requests

from utils.auth import OracleAuthentication


class OracleAPI:
    def __init__(self, auth: OracleAuthentication, endpoints: dict):
        self.auth = auth
        self.endpoints = endpoints
        self.response = {
            "procedure_response": {},
            "query_response": {},
            "batch_load_response": {},
            "deletion_response": {},
        }
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
        self.response["query_response"][query_object] = requests.get(
            query_url.format(query_id=query_id),
            headers={
                "Authorization": f"Bearer {self.auth.access_token}",
            },
        )

    def batch_load(
        self, load_object: Literal["products", "customers", "facts"], csv_file_path: str
    ):
        api_url = self.endpoints["batch_load"][load_object]["url"].format(
            base_url=self.auth.base_url
        )
        print(api_url)
        with open(csv_file_path, "rb") as csv_file:
            data = csv_file.read()
            self.response["batch_load_response"][load_object] = requests.post(
                api_url,
                headers={
                    "Authorization": f"Bearer {self.auth.access_token}",
                    "Content-Type": self.endpoints["batch_load"][load_object][
                        "headers"
                    ]["Content-Type"],
                },
                data=data,
            )
        return self.response["batch_load_response"][load_object]

    def execute_load(self, load_object: Literal["products", "customers", "facts"]):
        api_url = self.endpoints["execute_load"][load_object]["url"].format(
            base_url=self.auth.base_url
        )
        print(api_url)
        self.response["procedure_response"][load_object] = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {self.auth.access_token}",
                # "Content-Type": self.endpoints["execute_load"][load_object]["headers"][
                #     "Content-Type"
                # ],
            },
        )
        # self.check_load_error(self.response["procedure_response"][load_object])

        return self.response["procedure_response"][load_object]

    def delete_staging_table(
        self, table_name: Literal["products", "customers", "facts"]
    ):
        api_url = self.endpoints["delete_staging"][table_name]["url"].format(
            base_url=self.auth.base_url
        )
        self.response["deletion_response"][table_name] = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {self.auth.access_token}",
            },
        )
        return self.response["deletion_response"][table_name]

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
