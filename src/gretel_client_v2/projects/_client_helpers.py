import os

from gretel_client_v2.rest.configuration import Configuration
from gretel_client_v2.rest.api_client import ApiClient
from gretel_client_v2.rest.api.projects_api import ProjectsApi


_endpoint = os.getenv("GRETEL_API_ENDPOINT", "https://api-dev.gretel.cloud")
"""Gretel api endpoint"""

_api_key = os.getenv("GRETEL_API_KEY")
"""Gretel api key"""


def get_projects_api() -> ProjectsApi:
    configuration = Configuration(host=_endpoint)
    configuration.api_key['Authorization'] = _api_key
    api_client = ApiClient(configuration)
    return ProjectsApi(api_client)
