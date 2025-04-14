from typing import Optional
from unittest.mock import Mock

import pytest
import yaml

from gretel_client.safe_synthetics.blueprints import load_blueprint_or_config
from gretel_client.safe_synthetics.dataset import (
    SafeSyntheticDataset,
    SafeSyntheticDatasetFactory,
)
from gretel_client.test_utils import TestGretelApiFactory, TestGretelResourceProvider
from gretel_client.workflows.builder import WorkflowBuilder
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.workflows import Globals, Step, Workflow


@pytest.fixture
def tasks() -> Registry:
    return Registry()


@pytest.fixture
def stub_globals() -> Globals:
    return Globals()


@pytest.fixture
def ssd(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    stub_globals: Globals,
) -> SafeSyntheticDataset:
    builder = WorkflowBuilder(
        "proj_1", stub_globals, api_provider_mock, resource_provider_mock
    )
    return SafeSyntheticDataset(builder, Registry())


@pytest.fixture
def ssd_factory(
    api_provider_mock: TestGretelApiFactory,
    resource_provider_mock: TestGretelResourceProvider,
    stub_globals: Globals,
) -> SafeSyntheticDatasetFactory:
    builder = WorkflowBuilder(
        "proj_1", stub_globals, api_provider_mock, resource_provider_mock
    )
    builder_mock = Mock(return_value=builder)
    resource_provider_mock._workflows.builder = builder_mock

    return SafeSyntheticDatasetFactory(resource_provider_mock)


def extract_workflow_from_mock(dataset: SafeSyntheticDataset) -> Workflow:
    dataset.create()
    args = dataset._builder._data_api.exec_workflow_batch.call_args
    return Workflow(**args[0][0].workflow_config)


def get_step(name: str, steps: Optional[list[Step]]) -> Optional[Step]:
    if not steps:
        return None
    return next(step for step in steps if step.name == name)


def test_ssd_factory(ssd_factory: SafeSyntheticDatasetFactory, tasks: Registry):
    ssd = ssd_factory.from_data_source("file_1").transform().synthesize()
    workflow = extract_workflow_from_mock(ssd)

    steps = workflow.steps
    assert steps is not None
    assert len(steps) == 5

    assert steps[0].name == "read-data-source"
    assert steps[0].config == {"data_source": "file_1"}
    assert steps[0].task == "data_source"

    assert steps[1].name == "holdout"
    assert steps[1].inputs == []
    assert steps[1].task == "holdout"

    assert steps[2].name == "transform"
    assert steps[2].inputs == []
    assert steps[2].task == "transform"

    assert steps[3].name == "tabular-ft"
    assert steps[3].inputs == []
    assert steps[3].task == "tabular_ft"

    assert steps[4].name == "evaluate-safe-synthetics-dataset"
    assert steps[4].inputs == ["tabular-ft", "holdout"]
    assert steps[4].task == "evaluate_safe_synthetics_dataset"

    assert steps[0].config is not None
    assert steps[1].config is not None
    assert steps[2].config is not None
    assert steps[3].config is not None
    assert steps[4].config is not None


def test_ssd_num_records(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source(
        "file_1", holdout=None, use_data_source_step=False
    ).synthesize(num_records=10)

    steps = extract_workflow_from_mock(ssd).steps
    assert steps
    assert steps[0].config["generate"]["num_records"] == 10


def test_ssd_model_num_records(
    ssd_factory: SafeSyntheticDatasetFactory, tasks: Registry
):
    tab_ft_config = load_blueprint_or_config("tabular_ft/default")

    ssd = (
        ssd_factory.from_data_source("file_1", holdout=None, use_data_source_step=False)
        .synthesize("tabular_ft", tab_ft_config, num_records=10)
        .synthesize(tasks.TabularFt(), num_records=10)
    )

    steps = extract_workflow_from_mock(ssd).steps
    assert steps
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

    steps = extract_workflow_from_mock(ssd).steps
    assert steps
    assert steps[1].config == transform_config
    assert steps[2].config == tab_ft_config
    assert steps[3].config == tasks.TabularFt().model_dump()


def test_multiple_tasks(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source("file_1").transform().transform().synthesize()

    steps = extract_workflow_from_mock(ssd).steps
    assert steps
    assert [(s.name, s.inputs) for s in steps] == [
        ("read-data-source", None),
        ("holdout", []),
        ("transform", []),
        ("transform-1", []),
        ("tabular-ft", []),
        ("evaluate-safe-synthetics-dataset", ["tabular-ft", "holdout"]),
    ]


def test_multiple_tasks_no_hold_out(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = (
        ssd_factory.from_data_source("file_1", holdout=None)
        .transform()
        .transform()
        .synthesize()
    )

    steps = extract_workflow_from_mock(ssd).steps
    assert steps
    assert [(s.name, s.inputs) for s in steps] == [
        ("read-data-source", None),
        ("transform", []),
        ("transform-1", []),
        ("tabular-ft", []),
        ("evaluate-safe-synthetics-dataset", ["tabular-ft", "read-data-source"]),
    ]


def test_default_model_blueprint(ssd_factory: SafeSyntheticDatasetFactory):
    ssd = ssd_factory.from_data_source("file_1", holdout=None).synthesize("tabular_ft")
    steps = extract_workflow_from_mock(ssd).steps

    tabular_ft_step = get_step("tabular-ft", steps)
    assert tabular_ft_step
    assert tabular_ft_step.config == load_blueprint_or_config("tabular_ft/default")


def test_can_load_yaml_strings(ssd_factory: SafeSyntheticDatasetFactory):

    xf = """\
steps:
- columns:
    drop:
        - name: "temp_column"
    """

    synth_config = """\
train:
    group_training_examples_by: null
    order_training_examples_by: null
    params:
        num_input_records_to_sample: auto

generate:
    num_records: 5000

"""

    evaluate_config = """\
skip_attribute_inference_protection: true
"""

    ssd = (
        ssd_factory.from_data_source("file_1", holdout=None)
        .transform(xf)
        .synthesize("tabular_ft", synth_config)
        .evaluate(evaluate_config)
    )
    steps = extract_workflow_from_mock(ssd).steps

    transform = get_step("transform", steps)
    assert transform
    assert transform.config == yaml.safe_load(xf)

    tabular_ft = get_step("tabular-ft", steps)
    assert tabular_ft
    assert tabular_ft.config == yaml.safe_load(synth_config)

    evaluate_ssd_dataset = get_step("evaluate-safe-synthetics-dataset", steps)
    assert evaluate_ssd_dataset
    assert evaluate_ssd_dataset.config == yaml.safe_load(evaluate_config)
