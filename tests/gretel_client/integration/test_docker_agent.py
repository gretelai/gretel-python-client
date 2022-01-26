import threading
import time

from typing import Iterator

import pytest

from gretel_client.agents.agent import Agent, AgentConfig
from gretel_client.projects.jobs import ACTIVE_STATES, Status
from gretel_client.projects.projects import get_project, tmp_project

fake_pii = (
    "https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/fake_pii.csv"
)


@pytest.fixture
def agent_config() -> Iterator[AgentConfig]:
    with tmp_project() as project:
        yield AgentConfig(driver="docker", project=project.project_id)


def test_docker_agent(agent_config: AgentConfig, request):
    agent = Agent(config=agent_config)
    request.addfinalizer(agent.interupt)
    project = get_project(name=agent_config.project_id)
    model = project.create_model_obj("transform/default", fake_pii)
    model.submit_manual()

    def start():
        agent.start()

    t = threading.Thread(target=start)
    t.start()

    model._poll_job_endpoint()
    cur_wait = 0
    while model.status in ACTIVE_STATES and cur_wait < 30:
        time.sleep(1)  # this test will wait a max of 30 seconds
        model._poll_job_endpoint()
        cur_wait += 1

    agent.interupt()
    assert model.status == Status.COMPLETED