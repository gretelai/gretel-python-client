from unittest.mock import Mock

import pytest

from gretel_client.safe_synthetics.blueprints import load_blueprint_or_config
from gretel_client.safe_synthetics.dataset import (
    SafeSyntheticDataset,
    SafeSyntheticDatasetFactory,
)
from gretel_client.test_utils import TestGretelApiFactory, TestGretelResourceProvider
from gretel_client.workflows.builder import WorkflowBuilder
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.workflows import Step


@pytest.fixture
def tasks() -> Registry:
    return Registry()


@pytest.fixture
def ssd(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
) -> SafeSyntheticDataset:
    builder = WorkflowBuilder("proj_1", api_provider_mock, resource_provider_mock)
    return SafeSyntheticDataset(builder, Registry())


@pytest.fixture
def ssd_factory(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
) -> SafeSyntheticDatasetFactory:
    builder = WorkflowBuilder("proj_1", api_provider_mock, resource_provider_mock)
    builder_mock = Mock(return_value=builder)
    resource_provider_mock._workflows.builder = builder_mock

    return SafeSyntheticDatasetFactory(resource_provider_mock)


def test_ssd_factory(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source("file_1").transform().synthesize()
    ssd.create(wait=False)
    assert ssd.builder().to_workflow().steps == [
        Step(
            name="holdout", task="holdout", inputs=["file_1"], config={"holdout": 0.5}
        ),
        Step(
            name="transform",
            task="transform",
            inputs=["holdout"],
            config=load_blueprint_or_config("transform/default"),
        ),
        Step(
            name="tabular-ft",
            task="tabular_ft",
            inputs=["transform"],
            config=load_blueprint_or_config("tabular_ft/default"),
        ),
        Step(
            name="evaluate-ss-dataset",
            task="evaluate_ss_dataset",
            inputs=["tabular-ft", "holdout"],
            config={},
        ),
    ]


def test_ssd_num_records(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source("file_1", holdout=None).synthesize(
        num_records=10
    )
    ssd.create(wait=False)

    steps = ssd.builder().to_workflow().steps
    assert steps

    assert steps[0].config["generate"]["num_records"] == 10


def test_ssd_model_num_records(
    ssd_factory: SafeSyntheticDatasetFactory, tasks: Registry
):

    tab_ft_config = load_blueprint_or_config("tabular_ft/default")

    ssd = (
        ssd_factory.from_data_source("file_1", holdout=None)
        .synthesize("tabular_ft", tab_ft_config, num_records=10)
        .synthesize(tasks.TabularFt(), num_records=10)
    )
    ssd.create(wait=False)

    steps = ssd.builder().to_workflow().steps
    assert steps

    print(steps)

    assert steps[0].config["generate"]["num_records"] == 10
    assert steps[1].config["generate"]["num_records"] == 10


def test_ssd_model_configs(ssd_factory: SafeSyntheticDatasetFactory, tasks: Registry):
    transform_config = load_blueprint_or_config("transform/default")
    tab_ft_config = load_blueprint_or_config("tabular_ft/default")

    ssd = (
        ssd_factory.from_data_source("file_1", holdout=None)
        .transform(transform_config)
        .synthesize("tabular_ft", tab_ft_config)
        .synthesize(tasks.TabularFt())
        .evaluate(disable=True)
    )
    ssd.create(wait=False)

    steps = ssd.builder().to_workflow().steps
    assert steps

    assert steps[0].config == transform_config
    assert steps[1].config == tab_ft_config
    assert steps[2].config == tasks.TabularFt().model_dump()


def test_multiple_tasks(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source("file_1").transform().transform().synthesize()
    ssd.create(wait=False)

    steps = ssd.builder().to_workflow().steps
    assert steps
    assert [(s.name, s.inputs) for s in steps] == [
        ("holdout", ["file_1"]),
        ("transform-1", ["holdout"]),
        ("transform-2", ["transform-1"]),
        ("tabular-ft", ["transform-2"]),
        ("evaluate-ss-dataset", ["tabular-ft", "holdout"]),
    ]


def test_evaluate_can_be_disabled(ssd_factory: SafeSyntheticDatasetFactory): ...
