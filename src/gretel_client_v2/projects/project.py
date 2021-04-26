from gretel_client_v2.projects._client_helpers import get_model_api


class Project:

    def __init__(self):
        self.model_api = get_model_api()
