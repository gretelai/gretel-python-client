from unittest.mock import MagicMock, patch

import pytest

from gretel_client.config import DEFAULT_GRETEL_ARTIFACT_ENDPOINT, RunnerMode
from gretel_client.projects.artifact_handlers import (
    ArtifactsException,
    CloudArtifactsHandler,
    HybridArtifactsHandler,
)
from gretel_client.projects.projects import GretelProjectError, Project


@patch("gretel_client.projects.projects.get_session_config")
def test_hybrid_artifacts_handler_requires_custom_endpoint(get_session_config):
    config = MagicMock()
    config.artifact_endpoint = DEFAULT_GRETEL_ARTIFACT_ENDPOINT
    config.get_api.return_value = MagicMock()
    get_session_config.return_value = config

    with pytest.raises(ArtifactsException):
        Project(name="proj", project_id="123").hybrid_artifacts_handler

    config.artifact_endpoint = "s3://my-bucket"

    assert isinstance(
        Project(name="proj", project_id="123").hybrid_artifacts_handler,
        HybridArtifactsHandler,
    )


@patch("gretel_client.projects.projects.get_session_config")
@pytest.mark.parametrize(
    "runner_mode,artifact_endpoint,handler_type",
    [
        (RunnerMode.HYBRID, "s3://my-bucket", HybridArtifactsHandler),
        (RunnerMode.CLOUD, "gretel-cloud", CloudArtifactsHandler),
    ],
)
def test_default_aritfacts_handler_under_supported_runner_modes(
    get_session_config, runner_mode, artifact_endpoint, handler_type
):
    config = MagicMock(
        artifact_endpoint=artifact_endpoint,
        default_runner=runner_mode,
    )
    get_session_config.return_value = config
    project = Project(name="proj", project_id="123")

    assert isinstance(project.default_artifacts_handler, handler_type)


@patch("gretel_client.projects.projects.get_session_config")
@pytest.mark.parametrize(
    "runner_mode",
    [RunnerMode.LOCAL, RunnerMode.MANUAL],
)
def test_default_aritfacts_handler_raises_under_unsupported_runner_modes(
    get_session_config, runner_mode
):
    config = MagicMock(default_runner=runner_mode)
    get_session_config.return_value = config
    project = Project(name="proj", project_id="123")

    with pytest.raises(GretelProjectError):
        project.default_artifacts_handler
