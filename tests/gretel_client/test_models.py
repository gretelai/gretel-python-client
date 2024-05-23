import json
import tempfile

from pathlib import Path
from typing import Callable, List
from unittest.mock import MagicMock, patch

import pytest
import yaml

from gretel_client.cli.utils.parser_utils import RefData
from gretel_client.config import add_session_context, RunnerMode
from gretel_client.projects.artifact_handlers import (
    CloudArtifactsHandler,
    HybridArtifactsHandler,
)
from gretel_client.projects.exceptions import (
    DataSourceError,
    MaxConcurrentJobsException,
)
from gretel_client.projects.models import Model, ModelConfigError, read_model_config
from gretel_client.rest.exceptions import ApiException


@pytest.fixture
def transform_model_path(get_fixture: Callable) -> Path:
    return get_fixture("transforms_config.yml")


@pytest.fixture
def transform_local_data_source(get_fixture: Callable) -> Path:
    return get_fixture("account-balances.csv")


@pytest.fixture
def create_model_resp(get_fixture: Callable) -> dict:
    return json.loads(get_fixture("api/create_model.json").read_text())


@pytest.fixture
def get_model_resp(get_fixture: Callable) -> dict:
    return json.loads(get_fixture("api/completed_model_details.json").read_text())


@pytest.fixture
def create_record_handler_resp(get_fixture: Callable) -> dict:
    return json.loads(get_fixture("api/create_record_handler.json").read_text())


@pytest.fixture
def get_record_handler_resp(get_fixture: Callable) -> dict:
    return json.loads(
        get_fixture("api/completed_record_handler_details.json").read_text()
    )


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
def m(
    create_model_resp: dict,
    transform_model_path: Path,
    create_record_handler_resp: dict,
    get_record_handler_resp: dict,
    get_model_resp: dict,
) -> Model:
    projects_api = MagicMock()
    projects_api.get_model.return_value = get_model_resp
    projects_api.create_model.return_value = create_model_resp
    projects_api.create_artifact.return_value = create_artifact_resp
    projects_api.create_record_handler.return_value = create_record_handler_resp
    projects_api.get_record_handler.return_value = get_record_handler_resp
    project = MagicMock()
    project.projects_api = projects_api
    project.runner_mode = None
    project.default_artifacts_handler = CloudArtifactsHandler(
        projects_api, "proj_123", "project-name"
    )
    m = Model(project=project, model_config=transform_model_path)
    return m


@pytest.mark.parametrize("runner_mode", [RunnerMode.CLOUD, "cloud"])
def test_model_create(m: Model, create_model_resp: dict, runner_mode):
    assert m.model_id is None
    m.submit(runner_mode=runner_mode)
    m._projects_api.create_model.assert_called_once()  # type:ignore
    assert isinstance(m._data, dict)
    assert m.model_id == create_model_resp["data"]["model"]["uid"]
    assert m.status == create_model_resp["data"]["model"]["status"]
    assert m.worker_key == create_model_resp["worker_key"]


def test_model_submit_bad_runner_modes(m: Model):
    with pytest.raises(ValueError) as err:
        m.submit(runner_mode="foo")
    assert "Invalid runner_mode: foo" in str(err)

    with pytest.raises(ValueError) as err:
        m.submit(runner_mode=123)
    assert "Invalid runner_mode: 123" in str(err)


def test_model_submit_max_jobs_limit(m: Model):
    # If the ApiException from the api client is specifically due to max jobs,
    # we return a specific error type (MaxConcurrentJobsException)
    max_jobs_reason = "Maximum number of jobs created!"
    m.project.projects_api.create_model.side_effect = ApiException(
        status=400, reason=max_jobs_reason
    )
    with pytest.raises(MaxConcurrentJobsException) as err:
        m.submit(runner_mode="cloud")
    assert max_jobs_reason in str(err.value)
    assert err.value.reason == max_jobs_reason
    assert err.value.status == 400

    # All other exceptions from the api client are raised "as-is"
    other_reason = "Some other problem"
    m.project.projects_api.create_model.side_effect = ApiException(
        status=400, reason=other_reason
    )
    with pytest.raises(ApiException) as err:
        m.submit(runner_mode="cloud")
    assert other_reason in str(err.value)
    assert err.value.reason == other_reason
    assert err.value.status == 400


@patch("time.sleep")
def test_does_poll_status_and_logs(
    sleep_patch: MagicMock, m: Model, model_logs: List[dict]
):
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


def test_provenance(m: Model, transform_model_path):
    # When job provenance data is present, the body of the API request nests
    # the model config under "config" and the provenance data under "provenance"
    job_provenance = {
        "workflow_run_id": "wr_123",
        "workflow_action_name": "synthesize",
    }
    session = add_session_context(job_provenance=job_provenance)
    m.project.session = session

    m.submit(runner_mode=RunnerMode.CLOUD)

    body = m._projects_api.create_model.call_args.kwargs["body"]
    assert "schema_version" in body["config"].keys()
    assert "models" in body["config"].keys()
    assert body["provenance"] == job_provenance


def test_does_read_remote_model():
    synthetics_blueprint_raw_path = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel/synthetics/default.yml"  # noqa
    assert read_model_config(synthetics_blueprint_raw_path)
    with pytest.raises(ModelConfigError):
        read_model_config(f"{synthetics_blueprint_raw_path}/dsfljk")


def test_does_read_model_short_path():
    synthetics_blueprint_short_path = "synthetics/default"
    assert read_model_config(synthetics_blueprint_short_path)
    with pytest.raises(ModelConfigError):
        read_model_config("notfound")


def test_does_not_read_bad_local_data():
    with tempfile.NamedTemporaryFile(delete=False) as tmp_config:
        tmp_config.write(b"\tfoo")  # a regular string loads as YAML
        tmp_config.seek(0)
        with pytest.raises(ModelConfigError) as err:
            read_model_config(tmp_config.name)
        assert "YAML or JSON" in str(err)
    tmp_config.close()


def test_does_read_in_memory_model(transform_model_path: Path):
    config = yaml.safe_load(transform_model_path.read_bytes())
    assert read_model_config(config)


def test_does_read_local_model(transform_model_path: Path):
    assert read_model_config(transform_model_path)
    assert read_model_config(str(transform_model_path))


def test_does_populate_record_details(
    m: Model, create_record_handler_resp: dict, get_record_handler_resp: dict
):
    m._poll_job_endpoint()
    record_handler = m.create_record_handler_obj(
        data_source="path/to/datasource.csv",
    )
    record_handler.submit(
        runner_mode=RunnerMode.LOCAL,
    )
    assert (
        record_handler.status.value
        == create_record_handler_resp["data"]["handler"]["status"]
    )
    assert record_handler.worker_key == create_record_handler_resp["worker_key"]

    # Can access attributes of record handler fetched from cloud
    rh = m.get_record_handler(record_handler.record_id)
    assert rh.params == get_record_handler_resp["data"]["handler"]["config"]["params"]
    assert (
        rh.data_source
        == get_record_handler_resp["data"]["handler"]["config"]["data_source"]
    )


@pytest.mark.parametrize("num_records", [0, 1, 5, 9, 10, 11, 15, 19, 20, 21, 25, 50])
def test_goes_through_records(
    m: Model,
    num_records,
):
    m.submit(runner_mode=RunnerMode.LOCAL)

    def mock_record_handlers(status, skip, limit, *args, **kwargs):
        handlers = (
            [{"uid": 0} for _ in range(min(limit, num_records - skip))]
            if status == "completed"
            else []
        )
        return {"data": {"handlers": handlers}}

    m._projects_api.query_record_handlers.side_effect = mock_record_handlers

    assert len([h for h in m.get_record_handlers()]) == num_records


def test_billing_output(m: Model):
    m._poll_job_endpoint()
    # assert m.billing_details == {"total_billed_seconds": 60, "task_type": "cpu"}
    assert isinstance(m.billing_details, dict)


def test_xf_report_output(m: Model, get_fixture: Callable):
    report_json = get_fixture("xf_report_json.json.gz")
    peek = m.peek_report(str(report_json))
    expected_fields = [
        "training_time_seconds",
        "record_count",
        "field_count",
        "field_transforms",
    ]
    for field in expected_fields:
        assert field in peek.keys()


def test_synth_report_output(m: Model, get_fixture: Callable):
    report_json = get_fixture("synth_report_json.json.gz")
    m.model_config["models"][0] = {
        "synthetics": []
    }  # pretend the model stub is a synthetics model
    peek = m.peek_report(str(report_json))
    expected_fields = [
        "synthetic_data_quality_score",
        "field_correlation_stability",
        "principal_component_stability",
        "field_distribution_stability",
    ]
    for field in expected_fields:
        assert field in peek.keys()


def test_xf_report_summary(m: Model, get_fixture: Callable):
    report_json = get_fixture("xf_report_json.json.gz")
    m.model_config["models"][0] = {
        "transforms": {}
    }  # pretend the model stub is a transforms model
    summary = m.get_report_summary(str(report_json))
    # PROD-76, new summary format
    assert len(summary) == 1 and "summary" in summary
    expected_fields = {
        "training_time_seconds",
        "record_count",
        "field_count",
        "field_transforms",
        "value_transforms",
        "warnings",
    }
    found_fields = {d["field"] for d in summary["summary"]}
    assert expected_fields == found_fields


def test_synth_report_summary(m: Model, get_fixture: Callable):
    report_json = get_fixture("synth_report_json.json.gz")
    m.model_config["models"][0] = {
        "synthetics": []
    }  # pretend the model stub is a synthetics model
    summary = m.get_report_summary(str(report_json))
    # PROD-76, new summary format
    assert len(summary) == 1 and "summary" in summary
    expected_fields = {
        "synthetic_data_quality_score",
        "field_correlation_stability",
        "principal_component_stability",
        "field_distribution_stability",
    }
    found_fields = {d["field"] for d in summary["summary"]}
    assert expected_fields == found_fields


def test_can_name_model(m: Model):
    assert m.name == "my-awesome-model"
    new_name = "my-model"
    m.name = new_name
    assert m.name == new_name
    assert m.model_config["name"] == new_name
    assert m._local_model_config["name"] == new_name


def test_ref_data(m: Model, transform_local_data_source: Path):
    # Check non-existent ref data from config
    assert m.ref_data.is_empty

    # Cloud artifacts
    ref_data = RefData({"foo": "gretel_abc"})
    m.ref_data = ref_data
    assert m.ref_data == ref_data
    assert not m.external_ref_data

    # "Local file" but cannot verify its location on disk
    ref_data = RefData({"foo": "bar.csv"})
    m.ref_data = ref_data
    assert m.ref_data == ref_data
    assert m.external_ref_data
    with pytest.raises(DataSourceError):
        m.validate_ref_data()

    # Local file seated along with the config
    ref_data = RefData({"foo": transform_local_data_source.name})
    m.ref_data = ref_data
    check = m.ref_data.ref_dict["foo"]
    assert check.startswith(str(m._local_model_config_path.parent))
    assert m.external_ref_data
    m.validate_ref_data()


def test_does_write_artifacts_to_disk(tmpdir: Path, m: Model):
    base_endpoint = (
        "https://gretel-public-website.s3.us-west-2.amazonaws.com/tests/client/"
    )
    files = ["account-balances.csv", "report_json.json.gz", "model.tar.gz"]
    keys = ["data_preview", "report_json", "model"]
    m.get_artifacts = MagicMock(
        return_value=iter(zip(keys, [base_endpoint + f for f in files]))
    )
    m.submit(runner_mode="cloud")
    m.download_artifacts(str(tmpdir))
    for file in files:
        if file == "model.tar.gz":
            assert not (tmpdir / file).exists()
        else:
            assert (tmpdir / file).exists()


def test_does_write_artifacts_to_disk_hybrid(tmpdir: Path, m: Model):
    base_endpoint = (
        "https://gretel-public-website.s3.us-west-2.amazonaws.com/tests/client/"
    )
    files = ["account-balances.csv", "report_json.json.gz", "model.tar.gz"]
    keys = ["data_preview", "report_json", "model"]
    m.project.default_artifacts_handler = HybridArtifactsHandler(
        MagicMock(), m.project.project_id
    )
    m.get_artifacts = MagicMock(
        return_value=iter(zip(keys, [base_endpoint + f for f in files]))
    )
    m.submit(runner_mode="hybrid")
    m.download_artifacts(str(tmpdir))
    for file in files:
        if file == "model.tar.gz":
            assert not (tmpdir / file).exists()
        else:
            assert (tmpdir / file).exists()
