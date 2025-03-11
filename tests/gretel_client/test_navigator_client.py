import pytest

from gretel_client.navigator_client import Gretel
from gretel_client.navigator_client_protocols import GretelApiProviderProtocol
from gretel_client.test_utils import TestGretelApiFactory
from gretel_client.workflows.manager import TaskConfig


@pytest.fixture(scope="function")
def api_factory_mock() -> TestGretelApiFactory:
    return TestGretelApiFactory()


@pytest.fixture(scope="function")
def gretel(api_factory_mock: GretelApiProviderProtocol) -> Gretel:
    return Gretel(_api_factory=api_factory_mock)


def test_can_instantiate_client(gretel: Gretel):
    assert gretel is not None


def test_can_create_task_config(gretel: Gretel):
    name_generator = gretel.tasks.NameGenerator(num_records=10)
    assert isinstance(name_generator, TaskConfig)
    assert name_generator.num_records == 10


def test_workflow_interface(gretel: Gretel):
    assert gretel.workflows is not None
