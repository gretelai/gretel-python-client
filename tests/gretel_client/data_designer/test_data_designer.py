import json
import tempfile

from unittest.mock import MagicMock

import pytest
import yaml

from gretel_client.data_designer import DataDesigner
from gretel_client.data_designer.aidd_config import AIDDConfig
from gretel_client.data_designer.types import (
    CodeValidator,
    DataColumnFromPrompt,
    DataColumnFromSamplingT,
    Evaluator,
)
from gretel_client.workflows.configs.tasks import (
    ConcatDatasets,
    GenerateColumnFromTemplate,
    GenerateColumnsUsingSamplers,
    SampleFromDataset,
)


def test_data_designer_from_config(stub_aidd_config_str):
    dd = DataDesigner.from_config(
        gretel_resource_provider=MagicMock(), config=stub_aidd_config_str
    )
    assert isinstance(dd.get_column(column_name="code_id"), DataColumnFromSamplingT)
    assert isinstance(dd.get_column(column_name="text"), DataColumnFromPrompt)
    assert isinstance(dd.get_validator(validation_type="code"), CodeValidator)
    assert isinstance(dd.get_evaluator(evaluation_type="general"), Evaluator)

    assert dd.model_suite == "apache-2.0"
    assert dd.model_configs[0].alias == "my_own_code_model"


def test_column_operations():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    # chain addition of columns via add_column interface
    dd = dd.add_column(
        name="code_id",
        type="uuid",
        params={"prefix": "code_", "short_form": True, "uppercase": True},
    ).add_column(name="text", prompt="Write a description of fizzbuzz python code")
    assert len(dd.columns_from_sampling) == 1
    assert isinstance(dd.columns_from_sampling[0], DataColumnFromSamplingT)
    assert len(dd.columns_from_prompt) == 1
    assert isinstance(dd.columns_from_prompt[0], DataColumnFromPrompt)
    assert len(dd.categorical_columns) == 0

    # replace existing column
    dd = dd.add_column(name="text", prompt="Updated")
    assert dd.get_column(column_name="text").prompt == "Updated"

    # delete column by name
    dd = dd.delete_column("code_id")
    assert len(dd.columns_from_sampling) == 0

    # add categorical columns
    dd = dd.add_column(
        name="domain", type="category", params={"values": ["Healthcare", "Finance"]}
    ).add_column(
        name="topic",
        type="subcategory",
        params={
            "values": {
                "Healthcare": ["Web Development"],
                "Finance": ["Machine Learning"],
            }
        },
    )
    assert len(dd.categorical_columns) == 2

    # unsupported kwargs should raise an exception
    with pytest.raises(
        ValueError, match="Invalid keyword arguments {'unsupported_kwarg'}"
    ):
        dd.add_column(name="first_name", unsupported_kwarg="value")


def test_constraint_operations():
    dd = (
        DataDesigner(gretel_resource_provider=MagicMock())
        .add_column(name="age", type="gaussian", params={"mean": 35, "stdev": 5})
        .add_column(name="height", type="uniform", params={"low": 15, "high": 200})
        .add_constraint(
            target_column="age",
            type="scalar_inequality",
            params={"operator": "<", "rhs": 35},
        )
        # add another constraint to replace the old one
        .add_constraint(
            target_column="age",
            type="scalar_inequality",
            params={"operator": ">", "rhs": 30},
        )
        .add_constraint(
            target_column="height",
            type="column_inequality",
            params={"operator": ">", "rhs_column": "age"},
        )
    )
    assert dd.get_constraint(target_column="age").params["operator"] == ">"
    assert dd.get_constraint(target_column="age").params["rhs"] == 30

    # delete constraint by name
    dd.delete_constraint(target_column="height")
    assert dd.get_constraint(target_column="height") is None


def test_validator_operations():
    dd = (
        DataDesigner(gretel_resource_provider=MagicMock())
        .add_column(name="code", prompt="Write Python code")
        .add_validator(
            validation_type="code",
            settings={
                "code_lang": "python",
                "target_columns": ["code"],
                "result_columns": ["code_is_valid"],
            },
        )
        # add another validator to replace the old one
        .add_validator(
            validation_type="code",
            settings={
                "code_lang": "postgres",
                "target_columns": ["code"],
                "result_columns": ["code_is_valid"],
            },
        )
    )
    assert dd.get_validator(validation_type="code").settings.code_lang == "postgres"

    # delete validator by type
    dd.delete_validator(validation_type="code")
    assert dd.get_validator(validation_type="code") is None

    # invalid validator type raises exception
    with pytest.raises(ValueError, match="Unknown validator type"):
        dd.add_validator(validation_type="nonexistent", settings={})


def test_evaluator_operations():
    dd = (
        DataDesigner(gretel_resource_provider=MagicMock())
        .add_column(name="text", prompt="Write a description of python code")
        .add_column(name="code", prompt="Write Python code")
        .add_evaluator(
            evaluation_type="judge_with_llm",
            settings={
                "judge_template_type": "text_to_python",
                "instruction_column_name": "text",
                "response_column_name": "code",
            },
        )
        .add_evaluator(
            evaluation_type="general",
            settings={"llm_judge_column": "text_to_python"},
        )
        # add another evaluator to replace the old one
        .add_evaluator(
            evaluation_type="general",
            settings={"llm_judge_column": "text_to_python_updated"},
        )
    )
    assert (
        dd.get_evaluator(evaluation_type="general").settings.llm_judge_column
        == "text_to_python_updated"
    )

    # delete evaluator by type
    dd.delete_evaluator(evaluation_type="general")
    assert dd.get_evaluator(evaluation_type="general") is None

    # invalid evaluator type raises exception
    with pytest.raises(ValueError, match="Unknown evaluator type"):
        dd.add_evaluator(evaluation_type="nonexistent", settings={})


def test_export_operations(stub_aidd_config_str):
    dd = DataDesigner.from_config(
        gretel_resource_provider=MagicMock(), config=stub_aidd_config_str
    )
    # verify transformation back to AIDDConfig
    aidd_config = dd.to_aidd_config()
    assert isinstance(aidd_config, AIDDConfig)
    assert aidd_config.model_suite == dd.model_suite

    # verify transformation to dict
    aidd_config_dict = dd.to_config_dict()
    assert isinstance(aidd_config_dict, dict)
    assert aidd_config_dict["model_suite"] == dd.model_suite

    # verify config export to files
    with tempfile.NamedTemporaryFile(prefix="config", suffix=".json") as tmp_file:
        dd.export_config(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            assert json.loads(f.read())["model_suite"] == dd.model_suite

    with tempfile.NamedTemporaryFile(prefix="config", suffix=".yaml") as tmp_file:
        dd.export_config(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            deserialzied_config = yaml.safe_load(f.read())
            assert deserialzied_config["model_suite"] == dd.model_suite
            # verify enums are rendered as plain strings in the yaml file
            assert isinstance(deserialzied_config["model_suite"], str)

    with tempfile.NamedTemporaryFile(prefix="config", suffix=".yml") as tmp_file:
        dd.export_config(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            assert yaml.safe_load(f.read())["model_suite"] == dd.model_suite

    with pytest.raises(ValueError, match="The file extension must be one of"):
        dd.export_config(path="config.txt")


def test_workflow_builder_preview_integration(
    stub_aidd_config_str, mock_low_level_sdk_resources
):
    dd = DataDesigner.from_config(
        gretel_resource_provider=mock_low_level_sdk_resources.mock_resource_provider,
        config=stub_aidd_config_str,
    )
    _ = dd.preview(verbose_logging=True)
    mock_low_level_sdk_resources.mock_workflow_manager.builder.assert_called()
    call_kwargs = (
        mock_low_level_sdk_resources.mock_workflow_manager.builder.call_args.kwargs
    )
    globals = call_kwargs["globals"]
    assert globals.model_suite == dd.model_suite
    assert globals.model_configs == dd.model_configs

    mock_low_level_sdk_resources.mock_workflow_builder.add_step.assert_called()

    steps = [
        call[2]["step"]
        for call in mock_low_level_sdk_resources.mock_workflow_builder.add_step.mock_calls
    ]

    # verify steps
    assert isinstance(steps[0], SampleFromDataset)
    assert steps[0].num_samples == 10
    assert steps[0].with_replacement is True
    assert steps[0].strategy == "shuffle"

    assert isinstance(steps[1], GenerateColumnsUsingSamplers)
    assert len(steps[1].data_schema.columns) == 4
    expected_sampling_column_names = set([c.name for c in dd.columns_from_sampling])
    actual_sampling_column_names = set(
        set([col.name for col in steps[1].data_schema.columns])
    )
    assert expected_sampling_column_names == actual_sampling_column_names
    assert len(steps[1].data_schema.constraints) == 1
    assert steps[1].data_schema.constraints[0].target_column == "age"

    assert isinstance(steps[2], ConcatDatasets)

    assert isinstance(steps[3], GenerateColumnFromTemplate)
    assert steps[3].name == "text"

    assert isinstance(steps[4], GenerateColumnFromTemplate)
    assert steps[4].name == "code"

    # TODO: Add more assertions for validators and evaluators


def test_workflow_builder_run_integration(
    stub_aidd_config_str, mock_low_level_sdk_resources
):
    dd = DataDesigner.from_config(
        gretel_resource_provider=mock_low_level_sdk_resources.mock_resource_provider,
        config=stub_aidd_config_str,
    )
    dd.create(num_records=1000, workflow_run_name="test-run", wait_for_completion=True)
    mock_low_level_sdk_resources.mock_workflow_builder.run.assert_called_with(
        name="test-run", wait=True
    )
