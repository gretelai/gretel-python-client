import os
import uuid
import time
from functools import wraps

import pytest

from gretel_client.client import get_cloud_client, Client, BadRequest
from gretel_client.projects import Project


API_KEY = os.getenv('GRETEL_TEST_API_KEY')


if not API_KEY:
    raise AttributeError('GRETEL_TEST_API_KEY must be set!')


@pytest.fixture(scope='module')
def client():
    client = get_cloud_client('api-dev', API_KEY)
    # clear out any old projects that got leftover
    for p in client.search_projects():
        p.delete()
    return client

@pytest.fixture
def project(client):
    # NOTE: We create the empty project
    # in a fixture because in the event
    # of an assertion failure, the post
    # fixture hook will still delete the
    # project
    p: Project
    p = client.get_project(create=True)
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
def assert_check_for_records(project: Project, count=1):
    recs = project.sample()
    if len(recs) >= count:
        return True

@poll
def assert_check_record_count(project: Project, count):
    if project.record_count >= count:
        return True

@poll
def assert_check_field_count(project: Project, count):
    if project.field_count >= count:
        return True

def test_project_not_found(client: Client):
    with pytest.raises(BadRequest):
        client.get_project(name=uuid.uuid4().hex)


def test_project_not_available(client: Client):
    # use ``safecast`` that's one Gretel owns
    with pytest.raises(BadRequest):
        client.get_project(name='safecast', create=True)


def test_new_empty_project(client: Client, project: Project):
    # get a named project already, this should just return
    # as we already have a project instnace as a fixture
    client.get_project(name=project.name)

    assert not project.record_count
    assert not project.field_count
    assert not project.sample()

    # send via bulk
    project.send_bulk({'foo': 'bar'})
    assert_check_for_records(project)

    # send a record sync
    s, f = project.send({'foo2': 'bar2'})
    assert not f
    assert len(s) == 1
    assert_check_record_count(project, 2)

    # send some bad records
    s, f = project.send([1, 2, 3])
    assert not s
    assert len(f) == 3

    assert_check_field_count(project, count=2)
    assert_check_record_count(project, count=2)
    

def test_install_transformers(client: Client):
    client.install_transformers()
