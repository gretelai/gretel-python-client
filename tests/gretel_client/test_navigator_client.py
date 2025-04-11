from unittest.mock import ANY, Mock, patch

import pytest

from gretel_client.navigator_client import Gretel
from gretel_client.projects.exceptions import GretelProjectError
from gretel_client.projects.projects import Project
from gretel_client.test_utils import TestGretelApiFactory
from gretel_client.workflows.manager import TaskConfig


@pytest.fixture(scope="function")
def api_factory_mock() -> TestGretelApiFactory:
    return TestGretelApiFactory()


@pytest.fixture(scope="function")
@patch("gretel_client.navigator_client.get_project")
def gretel(api_factory_mock: TestGretelApiFactory) -> Gretel:
    return Gretel(_api_factory=api_factory_mock)


def test_can_instantiate_client(gretel: Gretel):
    assert gretel is not None


def test_can_create_task_config(gretel: Gretel):
    name_generator = gretel.tasks.NameGenerator(num_records=10)
    assert isinstance(name_generator, TaskConfig)
    assert name_generator.num_records == 10


def test_workflow_interface(gretel: Gretel):
    assert gretel.workflows is not None


@patch("gretel_client.navigator_client.get_project")
@patch("gretel_client.navigator_client.create_or_get_unique_project")
def test_creates_project(
    create_or_get_unique_project: Mock,
    get_project: Mock,
    api_factory_mock: TestGretelApiFactory,
):
    get_project.side_effect = GretelProjectError()

    Gretel(
        _api_factory=api_factory_mock,
        default_project_id="my-project",
    )

    create_or_get_unique_project.assert_called_once_with(name="my-project", session=ANY)
    get_project.assert_called_once_with(name="my-project", session=ANY)


@patch("gretel_client.navigator_client.get_project")
@patch("gretel_client.navigator_client.create_or_get_unique_project")
def test_reuses_project(
    create_or_get_unique_project: Mock,
    get_project: Mock,
    api_factory_mock: TestGretelApiFactory,
):
    get_project.side_effect = Mock(spec=Project)

    Gretel(
        _api_factory=api_factory_mock,
        default_project_id="my-project",
    )

    get_project.assert_called_with(name="my-project", session=ANY)
    create_or_get_unique_project.assert_not_called()
