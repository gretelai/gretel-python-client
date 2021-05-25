from pathlib import Path
from typing import Callable, List
from unittest.mock import MagicMock

import pytest
import yaml

from gretel_client_v2.projects import get_project
from gretel_client_v2.projects.models import (
    Model,
    ModelConfigError,
    RunnerMode,
    read_model_config,
)
from gretel_client_v2.projects.projects import Project


@pytest.fixture
def transform_model_path(get_fixture: Callable) -> Path:
    return get_fixture("transforms_config.yml")


@pytest.fixture
def transform_local_data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture
def create_model_resp() -> dict:
    return {
        "worker_key": "eyJwcm9",
        "data": {
            "model": {
                "uid": "92fb",
                "model_key": "grtm4e5d8",
                "runner_mode": "manual",
                "user_id": "5ece8962492fbf5bd66089f1",
                "project_id": "606b7895d4d0bffcc16a4da7",
                "logs": None,
                "status_history": {"created": "2021-05-02T18:23:59.002800"},
                "status": "created",
                "last_active_hb": None,
                "duration_minutes": None,
                "error_msg": None,
                "traceback": None,
                "model_type": "transform",
                "config": {},
            }
        },
    }


@pytest.fixture
def model_logs() -> List[dict]:
    return [
        {
            "ts": "2021-05-12T03:15:36.609784Z",
            "msg": "Starting transforms model training",
            "ctx": {},
            "seq": 1,
            "stage": "pre",
        },
        {
            "ts": "2021-05-12T03:15:36.610693Z",
            "msg": "Loading training data",
            "ctx": {},
            "seq": 2,
            "stage": "pre",
        },
        {
            "ts": "2021-05-12T03:15:36.908586Z",
            "msg": "Training data loaded",
            "ctx": {"record_count": 302, "field_count": 9},
            "seq": 3,
            "stage": "pre",
        },
        {
            "ts": "2021-05-12T03:15:36.908854Z",
            "msg": "Beginning transforms model training",
            "ctx": {},
            "seq": 4,
            "stage": "train",
        },
        {
            "ts": "2021-05-12T03:15:48.247923Z",
            "msg": "Saving model archive",
            "ctx": {},
            "seq": 5,
            "stage": "train",
        },
        {
            "ts": "2021-05-12T03:15:48.249298Z",
            "msg": "Generating data preview",
            "ctx": {"num_records": 100},
            "seq": 6,
            "stage": "run",
        },
        {
            "ts": "2021-05-12T03:15:48.249561Z",
            "msg": "Uploading artifacts to Gretel Cloud",
            "ctx": {},
            "seq": 7,
            "stage": "post",
        },
        {
            "ts": "2021-05-12T03:15:48.495138Z",
            "msg": "Model creation complete!",
            "ctx": {},
            "seq": 8,
            "stage": "post",
        },
    ]


@pytest.fixture
def create_artifact_resp() -> dict:
    return {
        "data": {
            "url": "https://gretel-proj-artifacts-us-east-2.s3.amazonaws.com/5fdzfdsf",
            "key": "gretel_dd3a7853b06343f79e645d27ca722a9e_account-balances.csv",
            "method": "PUT",
        }
    }


@pytest.fixture()
def m(create_model_resp: dict, transform_model_path: Path) -> Model:
    projects_api = MagicMock()
    projects_api.get_model.return_value = {"data": {"model": {}}}
    projects_api.create_model.return_value = create_model_resp
    projects_api.create_artifact.return_value = create_artifact_resp
    m = Model(project_id="123", model_config=transform_model_path)
    m._projects_api = projects_api
    return m


@pytest.fixture
def project():
    p = get_project(create=True)
    yield p
    p.delete()


def test_does_read_remote_model():
    synthetics_blueprint_raw_path = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel/synthetics/default.yml"  # noqa
    assert read_model_config(synthetics_blueprint_raw_path)
    with pytest.raises(ModelConfigError):
        read_model_config(f"{synthetics_blueprint_raw_path}/dsfljk")


def test_does_read_model_short_path():
    synthetics_blueprint_short_path = "synthetics/default"
    assert read_model_config(synthetics_blueprint_short_path)
    with pytest.raises(ModelConfigError):
        assert read_model_config("notfound")


def test_does_read_in_memory_model(transform_model_path: Path):
    config = yaml.safe_load(transform_model_path.read_bytes())
    assert read_model_config(config)


def test_does_read_local_model(transform_model_path: Path):
    assert read_model_config(transform_model_path)
    assert read_model_config(str(transform_model_path))


def test_model_submit(m: Model, create_model_resp: dict):
    assert m.model_id is None
    m.submit(runner_mode=RunnerMode.CLOUD)
    m._projects_api.create_model.assert_called_once()  # type:ignore
    assert m.model_id == create_model_resp["data"]["model"]["uid"]
    assert isinstance(m._data, dict)


def test_does_poll_status_and_logs(m: Model, model_logs: List[dict]):
    m.submit(runner_mode=RunnerMode.LOCAL)
    m._projects_api.get_model.side_effect = [  # type:ignore
        {"data": {"model": {"status": "created"}}},
        {"data": {"model": {"status": "pending"}}},
        {"data": {"model": {"status": "active"}, "logs": model_logs[0:1]}},
        {"data": {"model": {"status": "active"}, "logs": model_logs[0:2]}},
        {"data": {"model": {"status": "active"}, "logs": model_logs[0:5]}},
        {"data": {"model": {"status": "active"}, "logs": model_logs[0:6]}},
        {"data": {"model": {"status": "active"}, "logs": model_logs}},
        {"data": {"model": {"status": "completed"}}},
    ]
    updates = list(m.poll_logs_status())
    assert len(updates) == 8


def test_does_poll_model_logs(m: Model, model_logs: List[dict]):
    pass


# mark: integration
def test_does_get_model_from_id(project: Project, transform_model_path: Path):
    model: Model = project.create_model(transform_model_path)
    model.submit()
    assert model.model_id
    model_remote = Model(project_id=model.project_id, model_id=model.model_id)
    assert model_remote.status


# mark: integration
def test_does_upload_local_artifact(
    project: Project, transform_model_path: Path, transform_local_data_source: Path
):
    ds = str(transform_local_data_source)
    m = Model(project_id=project.project_id, model_config=transform_model_path)
    m.data_source = ds
    assert m.data_source == ds
    assert m.model_config["models"][0][m.model_type]["data_source"] == ds
    m._upload_data_source()
    assert m.data_source.startswith("gretel_")
