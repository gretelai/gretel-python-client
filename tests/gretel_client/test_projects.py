import os

from unittest.mock import MagicMock, patch

import pytest

from gretel_client.config import DEFAULT_GRETEL_ARTIFACT_ENDPOINT, RunnerMode
from gretel_client.projects.artifact_handlers import (
    ArtifactsException,
    CloudArtifactsHandler,
    HybridArtifactsHandler,
)
from gretel_client.projects.models import Model
from gretel_client.projects.projects import GretelProjectError, Project


def test_hybrid_artifacts_handler_requires_custom_endpoint():
    session = MagicMock()
    session.artifact_endpoint = DEFAULT_GRETEL_ARTIFACT_ENDPOINT
    session.get_api.return_value = MagicMock()

    # Merely obtaining the artifacts handler should not raise, only performing
    # an operation such as listing artifacts.
    handler = Project(
        name="proj", project_id="123", session=session
    ).hybrid_artifacts_handler
    with pytest.raises(ArtifactsException):
        handler.list_project_artifacts()

    session.artifact_endpoint = "s3://my-bucket"

    assert isinstance(
        Project(
            name="proj", project_id="123", session=session
        ).hybrid_artifacts_handler,
        HybridArtifactsHandler,
    )


@pytest.mark.parametrize(
    "runner_mode,artifact_endpoint,handler_type",
    [
        (RunnerMode.HYBRID, "s3://my-bucket", HybridArtifactsHandler),
        (RunnerMode.CLOUD, "gretel-cloud", CloudArtifactsHandler),
    ],
)
def test_default_aritfacts_handler_under_supported_runner_modes(
    runner_mode, artifact_endpoint, handler_type
):
    session = MagicMock(
        artifact_endpoint=artifact_endpoint,
        default_runner=runner_mode,
    )
    project = Project(name="proj", project_id="123", session=session)

    assert isinstance(project.default_artifacts_handler, handler_type)


@pytest.mark.parametrize(
    "runner_mode",
    [RunnerMode.LOCAL, RunnerMode.MANUAL],
)
def test_default_aritfacts_handler_raises_under_unsupported_runner_modes(runner_mode):
    session = MagicMock(default_runner=runner_mode)
    project = Project(name="proj", project_id="123", session=session)

    with pytest.raises(GretelProjectError):
        project.default_artifacts_handler


@patch("smart_open.open")
@patch("gretel_client.projects.artifact_handlers.BlobServiceClient")
@patch.dict(
    os.environ,
    {
        "AZURE_STORAGE_CONNECTION_STRING": "BlobEndpoint=https://test.blob.core.windows.net/"
    },
)
def test_get_artifact_handle_azure(
    blob_client_mock: MagicMock,
    smart_open_mock: MagicMock,
):
    session = MagicMock(
        artifact_endpoint="azure://my-bucket",
        default_runner=RunnerMode.HYBRID,
    )
    blob_client_mock_from_conn = MagicMock()
    blob_client_mock.from_connection_string.return_value = blob_client_mock_from_conn

    run = Model(
        Project(name="proj", project_id="123", session=session), model_id="my_model_id"
    )
    with run.get_artifact_handle("report_json"):
        smart_open_mock.assert_called_once_with(
            "azure://my-bucket/123/model/my_model_id/report_json.json.gz",
            "rb",
            transport_params={"client": blob_client_mock_from_conn},
        )


@patch("smart_open.open")
def test_get_artifact_handle_gs(
    smart_open_mock: MagicMock,
):
    session = MagicMock(
        artifact_endpoint="gs://my-bucket",
        default_runner=RunnerMode.HYBRID,
    )

    run = Model(
        Project(name="proj", project_id="123", session=session), model_id="my_model_id"
    )
    with run.get_artifact_handle("report_json"):
        smart_open_mock.assert_called_once_with(
            "gs://my-bucket/123/model/my_model_id/report_json.json.gz",
            "rb",
            transport_params={},
        )
