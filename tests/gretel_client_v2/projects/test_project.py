import pytest

from gretel_client_v2.projects import Project


@pytest.fixture
def project():
    return Project()


def test_project(project):
    assert project
