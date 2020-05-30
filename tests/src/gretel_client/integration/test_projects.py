import os
import uuid
import time
from functools import wraps

import pytest

from gretel_client.client import get_cloud_client, Client, BadRequest, Unauthorized
from gretel_client.projects import Project


API_KEY = os.getenv('GRETEL_TEST_API_KEY')


if not API_KEY:
    raise AttributeError('GRETEL_TEST_API_KEY must be set!')


@pytest.fixture(scope='module')
def client():
    client = get_cloud_client('api-dev', API_KEY)
    # clear out any old projects that got leftover
    for p in client.search_projects():
        try:
            p.delete()
        except Unauthorized:  # only delete projects that are owned
            pass
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


@poll
def assert_flush(project: Project):
    if not project.field_count and not project.record_count:
        return True


@poll
def assert_head(project: Project):
    check = project.head()
    if len(check) >= 2:
        return True


@poll
def assert_field_entity(project: Project, field, entity):
    fields = project.get_field_details()
    df = project.get_field_entities(as_df=True)

    if len(fields) < 2:
        return
    
    found_ent = False
    for f in fields:
        if f['field'] == field:
            for ent in f['entities']:
                if ent['label'] == entity:
                    found_ent = True

    if not found_ent:
        return

    df = df[df["entity_label"] == entity]

    if len(df) < 1:
        return

    return True

@poll
def assert_entity_count(project: Project, count: int):
    ents = project.entities
    if len(ents) == count:
        return True


def test_project_not_found(client: Client):
    with pytest.raises(BadRequest):
        client.get_project(name=uuid.uuid4().hex)


def test_simple_project_flow(client: Client, project: Project):
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

    # easy entity detection
    project.send({'foo': 'user@domain.com'})
    assert_field_entity(project, 'foo', 'email_address')

    # field filter
    check = project.get_field_details(entity='email_address')
    assert len(check) == 1

    assert_entity_count(project, 1)

    assert_head(project)

    # flush out all data
    project.flush()
    assert_flush(project)


def test_create_named_project(client: Client):
    name = uuid.uuid4().hex
    p = client.get_project(name=name, create=True)
    assert p.name == name
    assert p.description == ""

    # test search
    assert len(client.search_projects(query=name)) == 1
    p.delete()

    p = client.get_project(name=name, create=True, desc="this is super cool")
    assert p.name == name
    assert p.description == "this is super cool"

    p.delete()

    with pytest.raises(BadRequest):
        too_long = "".join([uuid.uuid4().hex for _ in range(20)])
        client.get_project(name=name, create=True, desc=too_long)


def test_install_transformers(client: Client):
    client.install_transformers()
