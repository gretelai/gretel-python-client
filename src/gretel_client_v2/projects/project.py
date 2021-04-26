from gretel_client_v2.projects._client_helpers import get_projects_api


class Project:

    def __init__(self):
        self.projects_api = get_projects_api()
