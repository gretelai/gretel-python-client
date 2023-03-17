import uuid

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client.projects import (
    create_or_get_unique_project,
    get_project,
    Project,
    search_projects,
)
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


def test_project_guid_url(project: Project):
    project = get_project(name=project.project_id)
    url = project.get_console_url()
    assert url.startswith("https://console-dev.gretel.ai")
    assert project.project_guid in url


def test_cannot_call_deleted_project():
    project = get_project(create=True)
    name = project.name
    project.delete()
    with pytest.raises(GretelProjectError):
        project.get_console_url()
    with pytest.raises(GretelProjectError):
        project.delete()
    with pytest.raises(GretelProjectError):
        get_project(name=name)


def test_upload_df_artifact(project: Project):
    input_df = pd.DataFrame([{"test_key": "test_value"}])
    art_key = project.upload_artifact(input_df)
    art_listing = project.artifacts
    assert len(art_listing) == 1
    df = pd.read_csv(project.get_artifact_link(art_key))
    assert df.equals(input_df)
    project.delete_artifact(art_key)
    assert len(project.artifacts) == 0


def test_does_get_artifacts(project: Project, get_fixture: Callable):
    art_to_upload = get_fixture("account-balances.csv")
    art_key = project.upload_artifact(art_to_upload)
    art_listing = project.artifacts
    assert len(art_listing) == 1
    df = pd.read_csv(project.get_artifact_link(art_key))
    assert df.equals(pd.read_csv(get_fixture("account-balances.csv")))
    project.delete_artifact(art_key)
    assert len(project.artifacts) == 0


def test_create_or_get_unique_project(request):
    name = uuid.uuid4().hex
    description = "sauce, that is awesome"

    # this project should not exist
    assert not search_projects(query=name)

    # first call should create it
    project = create_or_get_unique_project(name=name, desc=description)
    request.addfinalizer(project.delete)

    def _assert():
        assert project.name.startswith(name)
        assert len(project.name.split("-")) == 2
        assert project.description == description
        assert project.display_name == name
        assert len(search_projects(query=name)) == 1

    _assert()

    # next call should just return it
    project = create_or_get_unique_project(
        name=name, desc="NOPE", display_name="nope again"
    )

    _assert()
