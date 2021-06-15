import os
from typing import Callable

import pytest

from gretel_client_v2.config import (
    GRETEL_API_KEY,
    GretelClientConfigurationError,
    ClientConfig,
)


from gretel_client_v2.rest.api.projects_api import ProjectsApi
from gretel_client_v2.projects import Project, search_projects, get_project
from gretel_client_v2.projects.projects import GretelProjectError, tmp_project


def test_does_search_projects(project: Project):
    projects = search_projects()
    assert len(projects) > 0


def test_does_create_project(project: Project, projects_api: ProjectsApi):
    assert projects_api.get_project(project_id=project.project_id)


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


def test_does_check_project(dev_ep):
    config = ClientConfig(
        endpoint=dev_ep,
        api_key=os.getenv(GRETEL_API_KEY),
    )

    with pytest.raises(GretelClientConfigurationError):
        config.update_default_project("not_found")

    with tmp_project() as p:
        config.update_default_project(p.project_id)
        config = ClientConfig(
            endpoint=dev_ep,
            api_key=os.getenv(GRETEL_API_KEY),
            default_project_name=p.project_id,
        )
