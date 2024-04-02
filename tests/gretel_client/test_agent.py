import base64
import json

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator
from unittest import mock
from unittest.mock import call, MagicMock, patch

import pytest

from gretel_client.agents.agent import Agent, AgentConfig, AgentError, Job, Poller
from gretel_client.agents.drivers.docker import Docker
from gretel_client.agents.drivers.driver import GPU
from gretel_client.docker import CaCertFile, DEFAULT_GPU_CONFIG
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.rest.apis import JobsApi, ProjectsApi

worker_token_json = json.dumps({"model_key": "grtu123456abc"})
worker_token = base64.standard_b64encode(worker_token_json.encode("ascii"))


def get_mock_job(instance_type: str = "cpu-standard") -> dict:
    return {
        "run_id": "run-id",
        "job_type": "run",
        "container_image": "gretelai/transforms",
        "worker_token": worker_token,
        "instance_type": instance_type,
    }


@contextmanager
def patch_auth_api_calls_gen():
    with (
        patch("gretel_client.agents.agent.get_project") as get_project,
        patch("gretel_client.agents.agent.get_session_config") as get_session_config,
        patch("gretel_client.agents.agent.do_api_call") as do_api_call,
    ):
        get_project.return_value.project_id = "project1234"
        do_api_call.side_effect = [
            {"billing": {"me": {"service_limits": {"max_jobs_active": 5}}}},
            {"billing": {"me": {"service_limits": {"max_jobs_active": 25}}}},
        ]
        yield (get_project, get_session_config, do_api_call)


def patch_auth_api_calls(func):
    def inner(*args, **kwargs):
        with patch_auth_api_calls_gen():
            return func(*args, **kwargs)

    return inner


def default_agent_config() -> AgentConfig:
    return AgentConfig(
        driver="docker",
        max_workers=2,
        projects=["my-project-name"],
        log_factory=MagicMock(),
        creds=None,
    )


@pytest.fixture
def agent_config() -> Iterator[AgentConfig]:
    with patch_auth_api_calls_gen():
        yield default_agent_config()


def test_agent_config_fixture(agent_config: AgentConfig):
    assert agent_config.project_ids[0] == "project1234"


@patch_auth_api_calls
@patch("gretel_client.agents.agent.get_session_config")
@patch("gretel_client.agents.agent.get_driver")
@patch("requests.patch")
def test_agent_server_does_start(
    requests_patch: MagicMock,
    get_driver: MagicMock,
    get_session_config: MagicMock,
):
    mock_driver = MagicMock()
    get_driver.return_value = mock_driver
    jobs_api = MagicMock()
    project_api = MagicMock()

    def get_api(api, *args, **kwargs):
        if api == JobsApi:
            return jobs_api
        if api == ProjectsApi:
            return project_api
        assert False, "unexpected API requested"

    get_session_config.return_value.get_api.side_effect = get_api

    jobs_api.receive_one.side_effect = [
        {"data": {"job": get_mock_job()}},
        {"data": {"job": get_mock_job()}},
    ]
    agent_config = AgentConfig(
        driver="docker",
        max_workers=0,
        projects=["my-project-name"],
        log_factory=MagicMock(),
        creds=None,
    )
    server = Agent(agent_config)
    assert server._rate_limiter._max_active_jobs == 5
    server._rate_limiter._sleep_length = 0

    get_driver.assert_called_once_with(agent_config)

    get_driver.return_value.active.return_value = False

    class FinitePoller(Poller):
        test_count = 0

        def __next__(self):
            if self.test_count >= 2:
                raise StopIteration
            self.test_count += 1
            return super().__next__()

        @classmethod
        def wrap(cls, poller):
            return FinitePoller(
                jobs_api=jobs_api,
                rate_limiter=poller._rate_limiter,
                agent_config=poller._agent_config,
            )

    server._jobs = FinitePoller.wrap(server._jobs)

    server.start()

    assert server._rate_limiter._max_active_jobs == 25
    assert mock_driver.schedule.call_count == 2
    assert requests_patch.call_count == 2
    assert server._jobs_manager.active_jobs == 0


def test_agent_job_poller(agent_config: AgentConfig):
    jobs_api = MagicMock()
    rate_limiter = MagicMock()
    poller = Poller(jobs_api, rate_limiter, agent_config)

    job_data = get_mock_job()

    jobs_api.receive_one.return_value = {"data": {"job": job_data}}

    job = next(poller)

    jobs_api.receive_one.assert_called_once_with(
        project_ids=[agent_config.project_ids[0]], runner_modes=["manual"]
    )

    assert job
    assert job.uid == job_data["run_id"]
    assert job.job_type == job_data["job_type"]
    assert job.container_image == job_data["container_image"]
    assert job.worker_token == job_data["worker_token"]


@patch_auth_api_calls
def test_agent_job_poller_without_project_specified():
    agent_config = default_agent_config()
    agent_config.projects = []

    jobs_api = MagicMock()
    rate_limiter = MagicMock()
    poller = Poller(jobs_api, rate_limiter, agent_config)

    job_data = get_mock_job()

    jobs_api.receive_one.return_value = {"data": {"job": job_data}}

    job = next(poller)

    jobs_api.receive_one.assert_called_once_with(runner_modes=["manual"])

    assert job
    assert job.uid == job_data["run_id"]
    assert job.job_type == job_data["job_type"]
    assert job.container_image == job_data["container_image"]
    assert job.worker_token == job_data["worker_token"]


@patch_auth_api_calls
@patch("gretel_client.agents.agent.get_session_config")
@patch("gretel_client.agents.drivers.docker.build_container")
def test_job_with_ca_bundle(docker_client: MagicMock, get_session_config: MagicMock):
    cert = CaCertFile(cred_from_agent="/bundle/path")
    config = AgentConfig(
        driver="docker",
        creds=[cert],
    )
    job = Job.from_dict(get_mock_job(), config)

    assert job.env["REQUESTS_CA_BUNDLE"] == "/etc/ssl/agent_ca.crt"

    docker = Docker.from_config(config)
    docker.schedule(job)

    assert cert.volume in docker_client.mock_calls[0][2]["volumes"]


@patch_auth_api_calls
@patch("gretel_client.agents.drivers.docker.build_container")
def test_job_needs_gpu(build_container: MagicMock):
    config = AgentConfig(driver="docker", capabilities=[GPU])
    job = Job.from_dict(get_mock_job(instance_type="gpu-standard"), config)

    assert job.needs_gpu

    docker = Docker.from_config(config)
    docker.schedule(job)

    assert build_container.call_args_list[0][1]["device_requests"] == [
        DEFAULT_GPU_CONFIG
    ]


@patch_auth_api_calls
@patch("gretel_client.agents.agent.get_project")
def test_get_project_ids(get_project: MagicMock):
    @dataclass
    class lilproj:
        project_id: str

    config = AgentConfig(
        driver="docker",
        capabilities=[GPU],
        projects=["p1", "p2", "p3"],
        project_retry_limit=1,
    )

    get_project.side_effect = [
        lilproj(project_id="p1id"),
        GretelProjectError(),
        lilproj(project_id="p3id"),
    ]
    assert config.project_ids == ["p1id", "p3id"]
    get_project.assert_has_calls(
        [
            call(name="p1", session=mock.ANY),
            call(name="p2", session=mock.ANY),
            call(name="p3", session=mock.ANY),
        ]
    )

    get_project.reset_mock()
    get_project.side_effect = [
        lilproj(project_id="p2id"),
    ]
    assert config.project_ids == ["p1id", "p3id", "p2id"]
    get_project.assert_has_calls(
        [
            call(name="p2", session=mock.ANY),
        ]
    )

    get_project.reset_mock()
    assert config.project_ids == ["p1id", "p3id", "p2id"]
    get_project.assert_not_called()

    config.invalidate_project_ids()
    get_project.reset_mock()
    get_project.side_effect = [
        lilproj(project_id="p1id"),
        GretelProjectError(),
        lilproj(project_id="p3id"),
    ]
    assert config.project_ids == ["p1id", "p3id"]
    get_project.assert_has_calls(
        [
            call(name="p1", session=mock.ANY),
            call(name="p2", session=mock.ANY),
            call(name="p3", session=mock.ANY),
        ]
    )

    get_project.reset_mock()
    get_project.side_effect = [
        GretelProjectError(),
    ]
    with pytest.raises(GretelProjectError):
        assert config.project_ids == ["p1id", "p3id"]
    get_project.assert_has_calls(
        [
            call(name="p2", session=mock.ANY),
        ]
    )


@patch_auth_api_calls
def test_job_disables_cloud_logging():
    config = AgentConfig(driver="docker", disable_cloud_logging=True)
    job = Job.from_dict(get_mock_job(instance_type="gpu-standard"), config)
    assert job.params == {"--disable-cloud-logging": "", "--worker-token": worker_token}
    assert job.secret_env == {"GRETEL_WORKER_TOKEN": worker_token}


@patch("requests.patch")
def test_update_job_status_fails_no_error(
    requests_patch: MagicMock, agent_config: AgentConfig
):
    expected_exception = Exception("Whoopsie")
    requests_patch.side_effect = expected_exception
    agent = Agent(agent_config)
    mock_logger = MagicMock()
    agent._logger = mock_logger
    job = Job.from_dict(get_mock_job(instance_type="gpu-standard"), agent_config)
    agent._update_job_status(job)
    mock_logger.error.assert_called_once_with(
        "There was an error updating the job status: %s", expected_exception
    )


@patch_auth_api_calls
def test_agent_config_over_max():
    config = AgentConfig(driver="docker", max_workers=6)
    assert config.max_workers == 5


@patch_auth_api_calls
def test_agent_config_wrong_data_shape():
    with patch("gretel_client.agents.agent.do_api_call") as do_api_call:
        do_api_call.return_value = {}
        with pytest.raises(AgentError):
            AgentConfig(driver="docker", max_workers=10)


@patch_auth_api_calls
def test_agent_config_errors_out():
    with patch("gretel_client.agents.agent.do_api_call") as do_api_call:
        do_api_call.side_effect = Exception("nooooo")
        with pytest.raises(AgentError):
            AgentConfig(driver="docker", max_workers=10)


@patch_auth_api_calls
def test_agent_config_automatic():
    config = AgentConfig(driver="docker", max_workers=0)
    assert config.max_workers == 5


@patch_auth_api_calls
def test_agent_config_polls_new_option():
    config = AgentConfig(driver="k8s", max_workers=0)
    assert config.max_workers == 5
