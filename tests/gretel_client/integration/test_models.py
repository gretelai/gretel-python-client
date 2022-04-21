from pathlib import Path
from typing import Callable

import pytest
import smart_open

from gretel_client.cli.utils.parser_utils import RefData
from gretel_client.config import RunnerMode
from gretel_client.helpers import poll
from gretel_client.projects.common import ModelRunArtifact
from gretel_client.projects.docker import ContainerRun
from gretel_client.projects.jobs import Job, Status, WaitTimeExceeded
from gretel_client.projects.models import Model
from gretel_client.projects.projects import Project


@pytest.fixture
def transform_model_path(get_fixture: Callable) -> Path:
    return get_fixture("transforms_config.yml")


@pytest.fixture
def transform_local_data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


def run_job(job: Job, tmpdir: Path):
    docker_run = ContainerRun.from_job(job)
    docker_run.enable_debug()
    docker_run.start()
    return docker_run


def test_does_get_model_from_id(project: Project, transform_model_path: Path):
    model: Model = project.create_model_obj(transform_model_path)
    model.submit()
    assert model.model_id
    model_remote = Model(project=project, model_id=model.model_id)
    assert model_remote.status


def test_create_model_upload_artifacts(
    project: Project,
    transform_model_path: Path,
    transform_local_data_source: Path,
):
    data_refs = [
        {"foo": str(transform_local_data_source)},
        str(transform_local_data_source),
        [str(transform_local_data_source)],
    ]

    for ref_data in data_refs:
        model: Model = project.create_model_obj(
            transform_model_path,
            data_source=str(transform_local_data_source),
            ref_data=ref_data,
        )
        model.upload_data_source()
        model.upload_ref_data()
        assert model.data_source.startswith("gretel_")
        assert model.ref_data.values[0].startswith("gretel_")


def test_does_upload_local_artifact(
    project: Project, transform_model_path: Path, transform_local_data_source: Path
):
    ds = str(transform_local_data_source)
    m = Model(project=project, model_config=transform_model_path)
    m.data_source = ds
    # Use the same data source as ref data, just to validate e2e upload
    # TODO(jm): re-enable once REST APIs are stable
    # m.ref_data = RefData.from_list([ds])
    assert m.data_source == ds
    assert m.model_config["models"][0][m.model_type]["data_source"] == ds
    # assert m.model_config["models"][0][m.model_type]["ref_data"].get(0) == ds
    m.upload_data_source()
    # m.upload_ref_data()
    assert m.data_source.startswith("gretel_")
    # assert m.ref_data.values[0].startswith("gretel_")


def test_does_train_model_and_transform_records(
    project: Project,
    transform_model_path: Path,
    transform_local_data_source: Path,
):
    m = Model(project=project, model_config=transform_model_path)
    m.submit(runner_mode=RunnerMode.CLOUD)
    logs = list(m.poll_logs_status())
    assert len(logs) > 1
    assert m.status == Status.COMPLETED
    record_handler = m.create_record_handler_obj()
    record_handler.submit(
        params=None,
        action="transform",
        runner_mode=RunnerMode.CLOUD,
        data_source=str(transform_local_data_source),
        upload_data_source=True,
    )
    logs = list(record_handler.poll_logs_status())
    assert len(logs) > 1
    assert record_handler.status == Status.COMPLETED


def test_raises_wait_time_exceeded(
    project: Project,
    transform_model_path: Path,
):
    ...
    m = Model(project=project, model_config=transform_model_path)
    m.submit(runner_mode=RunnerMode.CLOUD)

    with pytest.raises(WaitTimeExceeded):
        # note: list is required here, because poll_logs_status() returns an iterator
        # and it may yield an item before throwing an exception
        list(m.poll_logs_status(wait=0))

    # poll logs until job is done
    logs = list(m.poll_logs_status())
    assert len(logs) > 1
    assert m.status == Status.COMPLETED


def test_does_get_synthetic_records(trained_synth_model: Model, request):
    handler = trained_synth_model.create_record_handler_obj()
    handler.submit(
        action="generate", runner_mode=RunnerMode.CLOUD, params={"num_records": 100}
    )
    request.addfinalizer(handler.delete)
    logs = list(handler.poll_logs_status())
    assert handler
    assert len(logs) > 1
    assert handler.status == Status.COMPLETED

    with smart_open.open(
        handler.get_artifact_link(ModelRunArtifact.DATA.value), "rb"  # type:ignore
    ) as syn_data:
        contents = syn_data.read()
        assert len(contents) > 0


def test_polls_with_helper(
    project: Project,
    transform_model_path: Path,
    capsys,
):
    m = Model(project=project, model_config=transform_model_path)
    m.submit(runner_mode=RunnerMode.CLOUD)
    poll(m)
    captured = capsys.readouterr()
    assert "Model creation complete" in captured.err
    assert m.status == Status.COMPLETED


def test_does_search_models(pre_trained_project: Project):
    models = list(pre_trained_project.search_models())
    assert len(models) > 0
    assert all([isinstance(m, Model) for m in models])

    models = list(pre_trained_project.search_models(factory=dict))
    assert len(models) > 0
    assert all([isinstance(m, dict) for m in models])
