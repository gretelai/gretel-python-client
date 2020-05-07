import pytest
from unittest.mock import Mock

from gretel_client.client import get_cloud_client, Client
from gretel_client.projects import Project


@pytest.fixture()
def client():
    return get_cloud_client('api', 'abcd123xyz')


def test_iter_records(client: Client):
    client.iter_records = Mock()
    p = Project(name='proj', client=client, project_id=123)
    p.iter_records()
    _, _, kwargs = p.client.iter_records.mock_calls[0]
    assert kwargs['project'] == 'proj'
    