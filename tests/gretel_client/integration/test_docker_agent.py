import threading
import time

from typing import Iterator

import pytest

from gretel_client.agents.agent import Agent, AgentConfig
from gretel_client.projects.jobs import ACTIVE_STATES, Status
from gretel_client.projects.projects import get_project, tmp_project

from .conftest import pytest_skip_on_windows

fake_pii = (
    "https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/fake_pii.csv"
)


@pytest.fixture
def agent_config() -> Iterator[AgentConfig]:
    with tmp_project() as project:
        yield AgentConfig(driver="docker", project=project.project_id)


@pytest_skip_on_windows
def test_docker_agent(agent_config: AgentConfig, request):
    agent = Agent(config=agent_config)
    request.addfinalizer(agent.interrupt)

    project = get_project(name=agent_config.project_id)
    model = project.create_model_obj("transform/default", fake_pii)

    model.submit_manual()
    request.addfinalizer(model.cancel)

    def start():
        agent.start()

    t = threading.Thread(target=start)
    t.start()

    model._poll_job_endpoint()
    cur_wait = 0
    passing_states = (Status.COMPLETED, Status.ACTIVE)
    while model.status in ACTIVE_STATES and cur_wait < 60:
        time.sleep(1)  # this test will wait a max of 60 seconds
        model._poll_job_endpoint()
        cur_wait += 1
        if model.status in passing_states:
            break

    agent.interrupt()
    assert model.status in passing_states
