from typing import Callable

import pytest

from gretel_client.projects import get_project, Project, search_projects
from gretel_client.projects.projects import GretelProjectError
from gretel_client.rest.api.projects_api import ProjectsApi


def test_does_search_projects(project: Project):
    projects = search_projects()
    assert len(projects) > 0


def test_does_create_project(project: Project, projects_api: ProjectsApi):
    assert projects_api.get_project(project_id=project.project_id)


def test_create_project_with_specific_name(request):
    name = "integ-test-project-1234"
    description = "Integration test project."

    project = get_project(create=True, name=name, desc=description)
    request.addfinalizer(project.delete)

    assert project.project_id is not None
    assert project.name == name
    assert project.description == description
    assert project.display_name == ""


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
