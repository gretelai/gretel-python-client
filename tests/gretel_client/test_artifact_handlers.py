import os
import tempfile

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from azure.storage.blob import BlobServiceClient

from gretel_client.config import DEFAULT_GRETEL_ARTIFACT_ENDPOINT
from gretel_client.projects.artifact_handlers import (
    _get_artifact_path_and_file_name,
    ArtifactsException,
    CloudArtifactsHandler,
    get_transport_params,
    hybrid_handler,
    HybridArtifactsHandler,
)
from gretel_client.projects.exceptions import DataSourceError


@pytest.fixture()
def endpoint():
    with tempfile.TemporaryDirectory() as endpoint:
        # Data Sources
        os.mkdir(f"{endpoint}/sources")
        os.mkdir(f"{endpoint}/sources/project_id")

        # Project
        os.mkdir(f"{endpoint}/project_id")

        # Model artifacts
        os.mkdir(f"{endpoint}/project_id/model")
        os.mkdir(f"{endpoint}/project_id/model/model_id")

        # Record handler artifacts
        os.mkdir(f"{endpoint}/project_id/run")
        os.mkdir(f"{endpoint}/project_id/run/record_id")

        yield endpoint


def test_cannot_make_hybrid_handler_with_default_artifact_endpoint():
    config = Mock(artifact_endpoint=DEFAULT_GRETEL_ARTIFACT_ENDPOINT)
    project = Mock(
        project_id="123",
        name="proj",
        projects_api=Mock(),
        client_config=config,
    )

    # Creating a handler (w/o performing any artifact operations) should
    # not raise an exception.
    handler = hybrid_handler(project)

    with pytest.raises(ArtifactsException):
        handler.list_project_artifacts()


def test_hybrid_created_with_custom_artifact_endpoint():
    config = Mock(artifact_endpoint="s3://my-bucket")
    project = Mock(
        project_id="123",
        name="proj",
        projects_api=Mock(),
        client_config=config,
    )

    assert isinstance(hybrid_handler(project), HybridArtifactsHandler)


@pytest.mark.parametrize(
    "key,value",
    [
        (
            "AZURE_STORAGE_CONNECTION_STRING",
            "BlobEndpoint=https://test.blob.core.windows.net/",
        ),
        ("OAUTH_STORAGE_ACCOUNT_NAME", "test-bucket"),
        ("AZURE_STORAGE_ACCOUNT_NAME", "test-bucket"),
        (
            "AZURE_BLOB_SAS_URL",
            "https://account.blob.core.windows.net/my-bucket/blob-name?abc=123",
        ),
    ],
)
def test_hybrid_created_with_azure_artifact_endpoint(key: str, value: str):
    with patch.dict(os.environ, {key: value}):
        artifact_endpoint = "azure://my-bucket"
        config = Mock(artifact_endpoint=artifact_endpoint)
        project = Mock(
            project_id="123",
            name="proj",
            projects_api=Mock(),
            client_config=config,
        )

        assert isinstance(hybrid_handler(project), HybridArtifactsHandler)
        transport_params = get_transport_params(artifact_endpoint)

        assert transport_params is not None
        assert isinstance(transport_params.get("client"), BlobServiceClient)


def test_hybrid_created_with_azure_artifact_different_domain():
    with patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_ACCOUNT_NAME": "test-storage",
            "AZURE_STORAGE_DOMAIN": ".usgovcloudapi.net",
        },
    ):
        artifact_endpoint = "azure://my-bucket"
        config = Mock(artifact_endpoint=artifact_endpoint)
        project = Mock(
            project_id="123",
            name="proj",
            projects_api=Mock(),
            client_config=config,
        )

        assert isinstance(hybrid_handler(project), HybridArtifactsHandler)
        transport_params = get_transport_params(artifact_endpoint)

        assert transport_params is not None
        client: BlobServiceClient = transport_params.get("client")
        assert isinstance(client, BlobServiceClient)
        assert client.account_name == "test-storage"
        assert client.url == "https://test-storage.blob.core.usgovcloudapi.net/"


def test_missing_environment_variable_error_with_azure_hybrid():
    with pytest.raises(
        ArtifactsException, match="Could not find Azure storage account credentials."
    ):
        get_transport_params("azure://my-bucket")


def test_get_artifact_path_and_file_name():
    # Test a DataFrame first
    dataframe = pd.DataFrame(data={"foo": [1, 2, 3], "bar": [4, 5, 6]})
    with _get_artifact_path_and_file_name(dataframe) as data:
        artifact_path, file_name = data
        assert Path(artifact_path).is_file()
        assert file_name.startswith("dataframe")
        assert file_name.endswith(".csv")

    # Test a local file
    with tempfile.NamedTemporaryFile() as tmp_file:
        with _get_artifact_path_and_file_name(tmp_file.name) as data:
            artifact_path, file_name = data
            assert artifact_path == tmp_file.name  # absolute path
            assert file_name == Path(tmp_file.name).name  # just file name


def test_hybrid_handler_limited_functionality():
    handler = HybridArtifactsHandler("endpoint", "project_id")

    with pytest.raises(ArtifactsException):
        handler.delete_project_artifact("key")

    with pytest.raises(ArtifactsException):
        handler.list_project_artifacts()

    with pytest.raises(ArtifactsException):
        handler.get_project_artifact_manifest("key")


@patch("uuid.uuid4")
def test_hybrid_upload_local_file_as_project_artifact(uuid4, endpoint):
    mock_uuid = Mock(hex="uuid")
    uuid4.return_value = mock_uuid

    with tempfile.NamedTemporaryFile(delete=False) as source:
        handler = HybridArtifactsHandler(endpoint, "project_id")
        artifact_path = handler.upload_project_artifact(source.name)

        filename = Path(source.name).name
        artifact_key = f"gretel_uuid_{filename}"
        sources_dir = f"{endpoint}/sources/project_id"
        expected_artifact_path = f"{sources_dir}/{artifact_key}"

        assert artifact_path == expected_artifact_path
        assert Path(artifact_path).exists()
        assert len(os.listdir(sources_dir)) == 1
        assert handler.get_project_artifact_link(artifact_key) == expected_artifact_path

        # ensure we do not re-upload existing artifacts
        # for testing we uploaded to a local temp directory (`endpoint`);
        # in actuality we'd have uploaded to an external object store,
        # so we patch here to simulate `artifact_key` referencing a remote file
        with patch(
            "gretel_client.projects.artifact_handlers.Path.exists", return_value=False
        ):
            handler.upload_project_artifact(artifact_key)
        assert len(os.listdir(sources_dir)) == 1

        source.close()
        os.unlink(source.name)


@patch("uuid.uuid4")
def test_hybrid_upload_dataframe_as_project_artifact(uuid4, endpoint):
    uuid4.side_effect = ["df-uuid", Mock(hex="gruuid")]

    dataframe = pd.DataFrame(data={"foo": [1, 2, 3], "bar": [4, 5, 6]})

    handler = HybridArtifactsHandler(endpoint, "project_id")
    artifact_key = handler.upload_project_artifact(dataframe)

    uploaded_artifact = Path(artifact_key)
    sources_dir = Path(endpoint) / "sources" / "project_id"
    expected_uploaded_artifact_path = (
        sources_dir / "gretel_gruuid_dataframe-df-uuid.csv"
    )

    assert uploaded_artifact.exists()
    assert uploaded_artifact == expected_uploaded_artifact_path
    assert len(os.listdir(sources_dir)) == 1


def test_hybrid_does_not_upload_remote_artifacts(endpoint):
    remote_data_source = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/sample_data/sample-synthetic-healthcare.csv"

    handler = HybridArtifactsHandler(endpoint, "project_id")
    artifact_key = handler.upload_project_artifact(remote_data_source)
    assert artifact_key == remote_data_source

    sources_dir = Path(endpoint) / "sources" / "project_id"
    assert len(os.listdir(sources_dir)) == 0


# Hybrid workers may have access to data sources that the client does not.
# It's not the client's responsibility to check access/auth to the data source.
# The client only checks if the data source is local (and therefore requires upload);
# if it is remote, just usher the value along and if the worker crashes, so be it.
def test_hybrid_passes_along_potentially_junk_data_source_value(endpoint):
    nonsense_data_source = "s3://not-a-real-bucket/or-if-it-is-we-cant-access-it.csv"

    handler = HybridArtifactsHandler(endpoint, "project_id")
    artifact_key = handler.upload_project_artifact(nonsense_data_source)
    assert artifact_key == nonsense_data_source

    sources_dir = Path(endpoint) / "sources" / "project_id"
    assert len(os.listdir(sources_dir)) == 0


# Like above, hybrid artifact validation only applies to local resources;
# remote resources are assumed to be valid by the client.
def test_hybrid_validation(endpoint, tmp_path):
    nonsense_data_source = "s3://not-a-real-bucket/or-if-it-is-we-cant-access-it.csv"
    nonexistent_local_file = "/this/file/does/not/exist.csv"
    valid_local_file = tmp_path / "data.csv"
    valid_local_file.write_text("h1,h2\n1,2")

    handler = HybridArtifactsHandler(endpoint, "project_id")

    assert handler.validate_data_source(nonsense_data_source)
    assert handler.validate_data_source(valid_local_file)
    with pytest.raises(DataSourceError):
        handler.validate_data_source(nonexistent_local_file)


def test_hybrid_artifact_link():
    handler = HybridArtifactsHandler("endpoint", "project_id")
    assert handler.get_project_artifact_link("key") == "endpoint/sources/project_id/key"


def test_hybrid_get_model_artifact_link(endpoint):
    report_artifact_path = f"{endpoint}/project_id/model/model_id/report.html.gz"
    with open(report_artifact_path, "w") as f:
        f.write("gzipped html")

    handler = HybridArtifactsHandler(endpoint, "project_id")
    model_artifact = handler.get_model_artifact_link("model_id", "report")

    assert model_artifact == report_artifact_path


def test_hybrid_get_record_handler_artifact_link(endpoint):
    report_artifact_path = (
        f"{endpoint}/project_id/run/record_id/run_report_json.json.gz"
    )
    with open(report_artifact_path, "w") as f:
        f.write("gzipped json")

    handler = HybridArtifactsHandler(endpoint, "project_id")
    model_artifact = handler.get_record_handler_artifact_link(
        "model_id", "record_id", "run_report_json"
    )

    assert model_artifact == report_artifact_path


def test_hybrid_get_job_artifacts_unrecognized_artifact_types_raise():
    handler = HybridArtifactsHandler("endpoint", "project_id")

    with pytest.raises(ArtifactsException):
        handler.get_model_artifact_link("model_id", "nonsense")

    with pytest.raises(ArtifactsException):
        handler.get_record_handler_artifact_link("model_id", "record_id", "nonsense")


def test_hybrid_download(endpoint):
    with tempfile.TemporaryDirectory() as output:
        report_artifact_path = f"{endpoint}/project_id/model/model_id/report.html.gz"
        with open(report_artifact_path, "w") as f:
            f.write("gzipped html")

        handler = HybridArtifactsHandler(endpoint, "project_id")
        model_artifact = handler.get_model_artifact_link("model_id", "report")

        output = Path(output)
        handler.download(model_artifact, output, "report", Mock())

        downloaded_file = output / "report.html.gz"
        assert downloaded_file.exists()


def test_cloud_data_source_passthrough_gretel_project_artifact():
    project_artifact = "gretel_1234567890_mydata.csv"
    projects_api = Mock()

    handler = CloudArtifactsHandler(projects_api, "proj_123", "projectname")
    artifact_key = handler.upload_project_artifact(project_artifact)
    assert artifact_key == project_artifact

    projects_api.create_artifact.assert_not_called()


def test_cloud_data_source_passthrough_public_gretel_artifact():
    remote_data_source = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/sample_data/sample-synthetic-healthcare.csv"
    projects_api = Mock()

    handler = CloudArtifactsHandler(projects_api, "proj_123", "projectname")
    artifact_key = handler.upload_project_artifact(remote_data_source)
    assert artifact_key == remote_data_source

    projects_api.create_artifact.assert_not_called()


def test_cloud_data_source_upload_as_project_artifact():
    projects_api = Mock()

    resp_key = "gretel_uuid_filename.csv"
    resp_url = "response-url"
    projects_api.create_artifact.return_value = {
        "data": {"key": resp_key, "url": resp_url}
    }

    handler = CloudArtifactsHandler(projects_api, "proj_123", "projectname")

    with (
        tempfile.NamedTemporaryFile(delete=False) as source,
        patch("gretel_client.projects.artifact_handlers.requests.put"),
    ):
        artifact_path = handler.upload_project_artifact(source.name)

    assert artifact_path == resp_key

    projects_api.create_artifact.assert_called_once()
