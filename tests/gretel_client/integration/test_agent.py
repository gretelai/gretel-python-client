from gretel_client.agents.agent import AgentConfig


def test_agent_config_from_user():
    config = AgentConfig(driver="docker")
    assert config.max_runtime_seconds and config.max_runtime_seconds > 0
