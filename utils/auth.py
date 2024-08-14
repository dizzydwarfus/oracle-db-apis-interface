import requests


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
