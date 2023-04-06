import base64
import json
import threading

from contextlib import contextmanager
from time import sleep
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from gretel_client.agents.agent import Agent, AgentConfig, AgentError, Job, Poller
from gretel_client.agents.drivers.docker import Docker
from gretel_client.agents.drivers.driver import GPU
from gretel_client.docker import CaCertFile, DEFAULT_GPU_CONFIG
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
def patch_auth_api_calls():
    with patch("gretel_client.agents.agent.get_project") as get_project, patch(
        "gretel_client.agents.agent.get_session_config"
    ) as get_session_config, patch(
        "gretel_client.agents.agent.do_api_call"
    ) as do_api_call:
        get_project.return_value.project_id = "project1234"
        get_session_config.return_value.get_api.return_value.users_me.return_value = {
            "data": {"me": {"service_limits": {"max_job_runtime": 100}}}
        }
        do_api_call.return_value = {
            "billing": {"me": {"service_limits": {"max_jobs_active": 5}}}
        }
        yield (get_project, get_session_config, do_api_call)


@pytest.fixture
def agent_config() -> Iterator[AgentConfig]:
    with patch_auth_api_calls():
        config = AgentConfig(
            driver="docker",
            max_workers=2,
            project="my-project-name",
            log_factory=MagicMock(),
            creds=None,
        )
        yield config


def test_agent_config_fixture(agent_config: AgentConfig):

    assert agent_config.project_id == "project1234"
    assert agent_config.max_runtime_seconds == 100


@patch("gretel_client.agents.agent.get_session_config")
@patch("gretel_client.agents.agent.get_driver")
@patch("requests.patch")
def test_agent_server_does_start(
    requests_patch: MagicMock,
    get_driver: MagicMock,
    get_session_config: MagicMock,
    agent_config: AgentConfig,
    request,
):
    mock_driver = MagicMock()
    get_driver.return_value = mock_driver
    jobs_api = MagicMock()
    project_api = MagicMock()

    get_session_config.return_value.get_api.side_effect = {
        JobsApi: jobs_api,
        ProjectsApi: project_api,
    }.get

    jobs_api.receive_one.side_effect = [
        {"data": {"job": get_mock_job()}},
        {"data": {"job": get_mock_job()}},
    ]

    server = Agent(agent_config)
    assert server._rate_limiter._max_active_jobs == 2

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
                jobs_api=poller._jobs_api,
                rate_limiter=poller._rate_limiter,
                agent_config=poller._agent_config,
            )

    server._jobs = FinitePoller.wrap(server._jobs)

    server.start()

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

    jobs_api.receive_one.assert_called_once_with(project_id=agent_config.project_id)

    assert job
    assert job.uid == job_data["run_id"]
    assert job.job_type == job_data["job_type"]
    assert job.container_image == job_data["container_image"]
    assert job.worker_token == job_data["worker_token"]


def test_agent_job_poller_without_project_specified(agent_config: AgentConfig):
    agent_config.project = None

    jobs_api = MagicMock()
    rate_limiter = MagicMock()
    poller = Poller(jobs_api, rate_limiter, agent_config)

    job_data = get_mock_job()

    jobs_api.receive_one.return_value = {"data": {"job": job_data}}

    job = next(poller)

    jobs_api.receive_one.assert_called_once_with()

    assert job
    assert job.uid == job_data["run_id"]
    assert job.job_type == job_data["job_type"]
    assert job.container_image == job_data["container_image"]
    assert job.worker_token == job_data["worker_token"]


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


@patch("gretel_client.agents.agent.get_session_config")
@patch("gretel_client.agents.drivers.docker.build_container")
def test_job_needs_gpu(build_container: MagicMock, get_session_config: MagicMock):
    config = AgentConfig(driver="docker", capabilities=[GPU])
    job = Job.from_dict(get_mock_job(instance_type="gpu-standard"), config)

    assert job.needs_gpu

    docker = Docker.from_config(config)
    docker.schedule(job)

    assert build_container.call_args_list[0][1]["device_requests"] == [
        DEFAULT_GPU_CONFIG
    ]


@patch("gretel_client.agents.agent.get_session_config")
def test_job_disables_cloud_logging(get_session_config: MagicMock):
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


def test_agent_config_over_max():
    with patch_auth_api_calls():
        config = AgentConfig(driver="docker", max_workers=6)
        assert config.max_workers == 5


def test_agent_config_wrong_data_shape():
    with patch_auth_api_calls():
        with patch("gretel_client.agents.agent.do_api_call") as do_api_call:
            do_api_call.return_value = {}
            with pytest.raises(AgentError):
                AgentConfig(driver="docker", max_workers=10)


def test_agent_config_errors_out():
    with patch_auth_api_calls():
        with patch("gretel_client.agents.agent.do_api_call") as do_api_call:
            do_api_call.side_effect = Exception("nooooo")
            with pytest.raises(AgentError):
                AgentConfig(driver="docker", max_workers=10)
