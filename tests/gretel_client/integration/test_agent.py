from gretel_client.agents.agent import AgentConfig


def test_agent_config_from_user():
    config = AgentConfig(driver="docker", max_workers=0)
    assert config.max_workers and config.max_workers > 0
