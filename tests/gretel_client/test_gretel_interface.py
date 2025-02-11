import json

from dataclasses import dataclass
from unittest.mock import call, Mock, patch

import pytest

from gretel_client.gretel.interface import Gretel
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.projects.projects import Project
from gretel_client.rest.apis import ProjectsApi
from gretel_client.rest.exceptions import ForbiddenException, NotFoundException


@dataclass
class MockResponse:
    status: int
    reason: str
    resp: dict

    def getheaders(self) -> dict:
        return {}

    @property
    def data(self) -> str:
        return json.dumps(self.resp)


@patch(
    "gretel_client.gretel.interface.get_me", return_value={"_id": "01234567_user_id"}
)
@patch("gretel_client.gretel.interface.add_session_context", return_value=None)
@patch("gretel_client.gretel.interface.configure_session", return_value=None)
@patch("gretel_client.gretel.interface.get_project")
def test_project_creation(
    mock_get_project, mock_configure_session, mock_add_session_context, mock_user_id
):
    """
    Test Gretel with existing project creates project with user_id suffix.
    """
    unique_project_name = "project-name-user_id"
    mock_get_project.side_effect = [
        NotFoundException(
            http_resp=MockResponse(
                reason="NotFound", status=404, resp={"message": "not found"}
            )
        ),
        Project(
            name=unique_project_name,
            project_id="project_id",
            project_guid="project_guid",
            session=mock_configure_session,
        ),
    ]
    gretel = Gretel(
        project_name="project-name",
        api_key="grtu...",
        endpoint="https://api-dev.gretel.cloud",
        skip_configure_session=True,
    )
    assert mock_get_project.call_count == 2
    assert mock_get_project.call_args_list[0] == call(
        name="project-name",
        display_name="project-name",
        desc=None,
        create=True,
        session=None,
    )
    assert mock_get_project.call_args_list[1] == call(
        name=unique_project_name,
        display_name="project-name",
        desc=None,
        create=True,
        session=None,
    )
    assert gretel._project.name == unique_project_name


@patch(
    "gretel_client.gretel.interface.get_me", return_value={"_id": "01234567_user_id"}
)
@patch("gretel_client.gretel.interface.add_session_context", return_value=None)
@patch("gretel_client.gretel.interface.configure_session", return_value=None)
@patch("gretel_client.projects.projects.get_session_config")
def test_project_creation_invalid(
    mock_get_session_config: Mock,
    mock_configure_session,
    mock_add_session_context,
    mock_user_id,
):
    projects_api = Mock()
    projects_api.get_project.side_effect = [
        NotFoundException(
            http_resp=MockResponse(
                reason="NotFound", status=404, resp={"message": "not found"}
            )
        ),
        ForbiddenException(
            http_resp=MockResponse(
                reason="NotFound", status=404, resp={"message": "forbidden"}
            )
        ),
    ]

    def get_api(api, *args, **kwargs):
        if api == ProjectsApi:
            return projects_api
        assert False, "unexpected API requested"

    mock_get_session_config.return_value.get_api.side_effect = get_api
    with pytest.raises(GretelProjectError) as e:
        Gretel(
            project_name="project_name",
            api_key="grtu...",
            endpoint="https://api-dev.gretel.cloud",
            skip_configure_session=True,
        )
    assert e.match("Project name 'project_name-user_id' is invalid")
