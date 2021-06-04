from typing import Callable

import pytest

from gretel_client_v2.config import get_session_config
from gretel_client_v2.rest.api.projects_api import ProjectsApi
from gretel_client_v2.projects import Project, search_projects, get_project
from gretel_client_v2.projects.projects import GretelProjectError


@pytest.fixture
def project(request):
    p = get_project(create=True)
    request.addfinalizer(p.delete)
    return p


@pytest.fixture
def api() -> ProjectsApi:
    return get_session_config().get_api(ProjectsApi)


def test_does_search_projects(project: Project):
    projects = search_projects()
    assert len(projects) > 0


def test_does_create_project(project: Project, api: ProjectsApi):
    assert api.get_project(project_id=project.project_id)


def test_does_get_project(project: Project):
    p = get_project(name=project.project_id)
    assert p.project_id == project.project_id


def test_cannot_call_deleted_project():
    project = get_project(create=True)
    project.delete()
    with pytest.raises(GretelProjectError):
        project.get_console_url()
    with pytest.raises(GretelProjectError):
        project.delete()


def test_does_get_artifacts(project: Project, get_fixture: Callable):
    art_to_upload = get_fixture("account-balances.csv")
    art_key = project.upload_artifact(art_to_upload)
    art_listing = project.artifacts
    assert len(art_listing) == 1
    project.delete_artifact(art_key)
    assert len(project.artifacts) == 0
