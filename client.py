import requests
from .models import SQLQuery, AuthResponse
from .exceptions import UnauthorizedError, MissingEntitlementsError

class UniversalSQLClient:
    def __init__(self, api_key: str, auth_url: str, sql_server_url: str):
        """
        Initialize the Universal API SQL Client.
        :param api_key: API key for authentication.
        :param auth_url: URL for the local auth server.
        :param sql_server_url: URL for the Universal SQL Server.
        """
        self.api_key = api_key
        self.auth_url = auth_url
        self.sql_server_url = sql_server_url

    def _pre_validate(self, query: SQLQuery):
        """
        Call the local auth server to pre-validate permissions.
        :param query: SQLQuery object containing the query to be validated.
        """
        auth_headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.post(f"{self.auth_url}/validate-query/", json=query.dict(), headers=auth_headers)
            if response.status_code == 401:
                raise UnauthorizedError("Unauthorized access - check API key or permissions")
            elif response.status_code == 403:
                raise MissingEntitlementsError("Missing entitlements - no permission for fields/datasets")
            auth_response = AuthResponse(**response.json())
            return auth_response
        except requests.RequestException as e:
            raise Exception(f"Failed to call local auth server: {e}")

    def _serialize_query(self, query: SQLQuery):
        """
        Serialize the SQL query to ensure it's structured properly.
        :param query: SQLQuery object containing the query to be serialized.
        """
        return query.dict()

    def execute_query(self, query: SQLQuery):
        """
        Execute a SQL query against the Universal SQL Server.
        :param query: SQLQuery object containing the query to be executed.
        """
        # Step 1: Pre-validate the query via the local auth server
        self._pre_validate(query)

        # Step 2: Serialize the query
        serialized_query = self._serialize_query(query)

        # Step 3: Send the query to the Universal API SQL Server
        sql_headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.post(f"{self.sql_server_url}/execute-query/", json=serialized_query, headers=sql_headers)
            if response.status_code == 200:
                return response.json()
            else:
                response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(f"Failed to execute query: {e}")
