import threading

from time import sleep
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from gretel_client.agents.agent import Agent, AgentConfig, Job, Poller
from gretel_client.agents.drivers.docker import Docker
from gretel_client.docker import CaCertFile
from gretel_client.rest.apis import JobsApi, ProjectsApi


def get_mock_job() -> dict:
    return {
        "run_id": "run-id",
        "job_type": "run",
        "container_image": "gretelai/transforms",
        "worker_token": "abcdef1243",
    }


@pytest.fixture
def agent_config() -> Iterator[AgentConfig]:
    with patch("gretel_client.agents.agent.get_project") as get_project, patch(
        "gretel_client.agents.agent.get_session_config"
    ) as get_session_config:
        get_project.return_value.project_id = "project1234"
        get_session_config.return_value.get_api.return_value.users_me.return_value = {
            "data": {"me": {"service_limits": {"max_job_runtime": 100}}}
        }
        config = AgentConfig(
            driver="docker",
            max_workers=1,
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
def test_agent_server_does_start(
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
    request.addfinalizer(server.interupt)

    get_driver.assert_called_once_with(agent_config)

    def start():
        server.start(cooloff=0.1)

    t = threading.Thread(target=start)
    t.start()

    get_driver.return_value.active.return_value = False

    cur_wait = 0
    while mock_driver.schedule.call_count < 2 and cur_wait < 20:
        sleep(1)  # loop will wait a max of 2 seconds
        cur_wait += 1

    server.interupt()
    assert mock_driver.schedule.call_count == 2
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
