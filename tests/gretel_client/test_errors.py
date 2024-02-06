import json

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Type
from unittest.mock import ANY, MagicMock, patch

import pytest
import urllib3.exceptions

from click.testing import CliRunner

import gretel_client.cli.errors
import gretel_client.rest.exceptions
import gretel_client.rest.rest

from gretel_client.cli.cli import cli
from gretel_client.cli.errors import (
    _ErrorHandler,
    HandleApiClientError,
    HandleConnectionError,
    HandleGretelResourceNotFoundError,
    HandlePythonError,
)
from gretel_client.projects.models import ModelConfigError, ModelNotFoundError


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


@dataclass
class MockHTTPSConnectionPool:
    host: str
    port: int = 443


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@contextmanager
def error_handler(handler: Type[_ErrorHandler]) -> Iterator[MagicMock]:
    with patch(
        f"{handler.__module__}.{handler.__name__}", wraps=handler
    ) as error_handler_spy:
        yield error_handler_spy


@patch("gretel_client.cli.projects.search_projects")
def test_bad_api_keys(search_projects: MagicMock, runner: CliRunner):
    search_projects.side_effect = gretel_client.rest.exceptions.ApiException(
        http_resp=MockResponse(
            reason="Unauthorized",
            status=401,
            resp={
                "message": "Unauthorized",
            },
        ),
    )

    with error_handler(HandleApiClientError) as spy:
        cmd = runner.invoke(cli, ["projects", "search"])

    spy.assert_called_once_with(ANY, search_projects.side_effect)
    assert cmd.exit_code == 1
    assert "please check your credentials" in cmd.output
    assert "Unauthorized" in cmd.output


@patch("gretel_client.cli.projects.search_projects")
def test_bad_endpoint(search_projects: MagicMock, runner: CliRunner):
    search_projects.side_effect = urllib3.exceptions.MaxRetryError(
        pool=MockHTTPSConnectionPool(host="https://gretel.cloud"),
        url="/projects?limit=200",
    )

    with error_handler(HandleConnectionError) as spy:
        cmd = runner.invoke(cli, ["projects", "search"])

    spy.assert_called_once_with(ANY, search_projects.side_effect)
    assert cmd.exit_code == 1
    assert "Connection error" in cmd.output


@patch("gretel_client.cli.projects.create_project")
def test_api_validation_error_project_name(
    create_project: MagicMock, runner: CliRunner
):
    create_project.side_effect = gretel_client.rest.exceptions.ApiException(
        http_resp=MockResponse(
            reason="Bad Request",
            status=400,
            resp={
                "message": "Invalid JSON Payload!",
                "context": {"name": ["String does not match expected pattern."]},
                "error_id": None,
            },
        ),
    )

    with error_handler(HandleApiClientError) as spy:
        cmd = runner.invoke(cli, ["projects", "create", "--name", "1notdnscompliant"])

    spy.assert_called_once_with(ANY, create_project.side_effect)
    assert cmd.exit_code == 1
    assert "name" in cmd.output
    assert "String does not match expected pattern." in cmd.output


@patch("gretel_client.cli.common.get_project")
def test_bad_model_filepath(get_project: MagicMock, runner: CliRunner):
    config_error = ModelConfigError("Model 'bad-model-id' not found")
    get_project.return_value.create_model_obj.side_effect = config_error
    with error_handler(HandlePythonError) as spy:
        cmd = runner.invoke(
            cli,
            ["models", "create", "--config", "not-found.txt", "--project", "1234"],
        )
    spy.assert_called_once_with(ANY, config_error)
    assert cmd.exit_code == 1
    assert "bad-model-id" in cmd.output


@patch("gretel_client.cli.common.get_project")
def test_model_not_found(get_project: MagicMock, runner: CliRunner):
    job = MagicMock()
    job.job_type = "model"
    job.id = "model-id-1"
    job.project.project_id = "project-id-1"
    not_found = ModelNotFoundError(job=job)
    get_project.return_value.get_model.side_effect = not_found
    with error_handler(HandleGretelResourceNotFoundError) as spy:
        cmd = runner.invoke(
            cli,
            [
                "records",
                "generate",
                "--model-id",
                "60dca3d09c03f7c6edadee91",
                "--project",
                "not-found",
            ],
        )
    spy.assert_called_once_with(ANY, not_found)
    assert "The model 'model-id-1' could not be found" in cmd.output
    assert cmd.exit_code == 1


def test_cli_bad_parameters():
    runner = CliRunner()
    cmd = runner.invoke(cli, ["projects", "search", "--bad-param", "value"])
    assert cmd.output.startswith("Usage")
    assert cmd.exit_code == 2
