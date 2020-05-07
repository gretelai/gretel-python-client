import os
import uuid
import time
from functools import wraps

import pytest

from gretel_client.client import get_cloud_client, Client, BadRequest
from gretel_client.projects import Project

API_KEY = os.getenv('GRETEL_API_KEY')


if not API_KEY:
    raise AttributeError('GRETEL_API_KEY must be set!')


@pytest.fixture(scope='module')
def client():
    client = get_cloud_client('api-dev', API_KEY)
    # clear out any old projects that got leftover
    for p in client.get_projects()['data']['projects']:
        client._delete_project(p['_id'])
    return client

@pytest.fixture
def project(client):
    # NOTE: We create the empty project
    # in a fixture because in the event
    # of an assertion failure, the post
    # fixture hook will still delete the
    # project
    p: Project
    p = client.get_project()
    yield p
    p.delete()


def poll(func):
    @wraps(func)
    def handler(*args, **kwargs):
        count = 0
        while count < 10:
            if func(*args, **kwargs):
                return True
            else:
                time.sleep(1)
                count += 1
        raise RuntimeError('Timeout while polling API')
    return handler

@poll
def check_for_records(project: Project, count=1):
    recs = project.sample()
    if len(recs) >= count:
        return True


def test_project_not_found(client: Client):
    with pytest.raises(BadRequest):
        client.get_project(name=uuid.uuid4().hex)


def test_new_empty_project(client: Client, project: Project):
    # get a named project already, this should just return
    # as we already have a project instnace as a fixture
    client.get_project(name=project.name)

    assert not project.record_count
    assert not project.field_count
    assert not project.sample()

    # send a record!
    project.send_records({'foo': 'bar'})
    assert check_for_records(project)
