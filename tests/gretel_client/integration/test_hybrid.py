import json

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest

from gretel_client._hybrid.config import _HybridSessionConfig
from gretel_client._hybrid.creds_encryption import (
    CredentialsEncryption,
    NoCredentialsEncryption,
)
from gretel_client.config import get_session_config
from gretel_client.projects import get_project, Project
from gretel_client.projects.projects import Model, RunnerMode
from gretel_client.rest.api.projects_api import Artifact, ProjectsApi
from gretel_client.rest_v1.api.connections_api import (
    ConnectionsApi,
    CreateConnectionRequest,
)
from gretel_client.rest_v1.api.workflows_api import CreateWorkflowRequest, WorkflowsApi


@pytest.fixture
def legacy_project():
    """
    Returns a legacy (i.e., non-hybrid) project. This is required by some tests that attempt
    to create cloud models.
    """
    session = get_session_config()
    if isinstance(session, _HybridSessionConfig):
        session = session._delegate
    p = get_project(create=True, session=session)
    p = get_project(name=p.name)
    yield p
    p.delete()


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_project_artifact_fails(project: Project):
    session = get_session_config()
    projects_api = session.get_api(ProjectsApi)
    with pytest.raises(
        Exception, match="project artifact upload is disabled in Hybrid mode"
    ):
        projects_api.create_artifact(
            project_id=project.project_id, artifact=Artifact(filename="test.csv")
        )


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_cloud_model_fails(legacy_project: Project, get_fixture):
    model: Model = legacy_project.create_model_obj(get_fixture("llama2_config.yml"))

    with pytest.raises(ValueError, match="only 'hybrid' is allowed"):
        model.submit_cloud(dry_run=True)
    with pytest.raises(ValueError, match="only 'hybrid' is allowed"):
        model.submit(runner_mode=RunnerMode.CLOUD, dry_run=True)


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_hybrid_model_succeeds(project: Project, get_fixture):
    model: Model = project.create_model_obj(get_fixture("llama2_config.yml"))

    job = model.submit_hybrid(dry_run=True)
    assert job.runner_mode == RunnerMode.HYBRID.value

    model = project.create_model_obj(get_fixture("llama2_config.yml"))

    job = model.submit(runner_mode=RunnerMode.HYBRID, dry_run=True)
    assert job.runner_mode == RunnerMode.HYBRID.value


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_default_runner_mode_is_hybrid(project: Project, get_fixture):
    session = get_session_config()
    assert session.default_runner == RunnerMode.HYBRID

    model: Model = project.create_model_obj(get_fixture("llama2_config.yml"))
    job = model.submit(dry_run=True)
    assert job.runner_mode == RunnerMode.HYBRID.value


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_cloud_workflow_fails(project: Project, get_fixture):
    workflows_api = get_session_config().get_v1_api(WorkflowsApi)
    with pytest.raises(
        ValueError,
        match="only workflows with runner mode RUNNER_MODE_HYBRID can be created",
    ):
        workflows_api.create_workflow(
            CreateWorkflowRequest(
                name="my-workflow",
                project_id=project.project_guid,
                config_text=get_fixture("workflows/workflow.yaml").read_text(),
                runner_mode="RUNNER_MODE_CLOUD",
            )
        )


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_workflow_defaults_to_hybrid(project: Project, get_fixture):
    workflows_api = get_session_config().get_v1_api(WorkflowsApi)
    workflow = workflows_api.create_workflow(
        CreateWorkflowRequest(
            name="my-workflow",
            project_id=project.project_guid,
            config_text=get_fixture("workflows/workflow.yaml").read_text(),
        )
    )
    assert workflow.runner_mode == "RUNNER_MODE_HYBRID"


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_connection_fails_no_creds_encryption(project: Project, get_fixture):
    connections_api = get_session_config().get_v1_api(ConnectionsApi)
    connection_config = json.loads(
        get_fixture("connections/test_connection.json").read_text()
    )
    with pytest.raises(
        NotImplementedError, match="no credentials encryption is configured"
    ):
        connections_api.create_connection(
            CreateConnectionRequest(
                project_id=project.project_guid,
                **connection_config,
            )
        )


@pytest.mark.gretel_hybrid(creds_encryption=NoCredentialsEncryption())
def test_create_connection_fails_succeeds_with_pre_encrypted_creds(
    project: Project, get_fixture
):
    connections_api = get_session_config().get_v1_api(ConnectionsApi)
    connection_config = json.loads(
        get_fixture("connections/test_connection_pre_encrypted_creds.json").read_text()
    )
    conn = connections_api.create_connection(
        CreateConnectionRequest(
            project_id=project.project_guid,
            **connection_config,
        )
    )
    assert conn.customer_managed_credentials_encryption


class FakeAWSKMSCredentialsEncryption(CredentialsEncryption):
    def _encrypt_payload(self, payload: bytes) -> bytes:
        return "fake payload".encode("utf-8")

    def make_encrypted_creds_config(self, ciphertext_b64: str) -> dict:
        return {
            "aws_kms": {
                "data": ciphertext_b64,
                "key_arn": "arn:aws:kms:us-west-2:123456789:key/00000000-0000-0000-0000-000000000000",
            },
        }


@pytest.mark.gretel_hybrid(creds_encryption=FakeAWSKMSCredentialsEncryption())
def test_create_connection_fails_succeeds_with_encryption(
    project: Project, get_fixture
):
    connections_api = get_session_config().get_v1_api(ConnectionsApi)
    connection_config = json.loads(
        get_fixture("connections/test_connection.json").read_text()
    )
    conn = connections_api.create_connection(
        CreateConnectionRequest(
            project_id=project.project_guid,
            **connection_config,
        )
    )
    assert conn.customer_managed_credentials_encryption
