import pytest

from gretel_client_v2.config import get_session_config
from gretel_client_v2.rest.api.projects_api import ProjectsApi
from gretel_client_v2.projects import Project, search_projects, get_project
from gretel_client_v2.projects.projects import GretelProjectError


@pytest.fixture
def project():
    p = get_project(create=True)
    yield p
    p.delete()


@pytest.fixture
def api() -> ProjectsApi:
    return get_session_config().get_api(ProjectsApi)


def test_doest_search_projects():
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
