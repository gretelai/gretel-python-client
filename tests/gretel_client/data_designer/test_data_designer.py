import json
import tempfile

from unittest.mock import MagicMock

import pytest
import yaml

from pydantic import BaseModel

from gretel_client.data_designer.aidd_config import AIDDConfig
from gretel_client.data_designer.data_designer import (
    DataDesigner,
    DataDesignerValidationError,
    get_column_from_kwargs,
)
from gretel_client.data_designer.types import (
    CodeValidationColumn,
    EvaluationReportT,
    ExpressionColumn,
    LLMCodeColumn,
    LLMJudgeColumn,
    LLMStructuredColumn,
    LLMTextColumn,
    ProviderType,
    SamplerColumn,
)
from gretel_client.workflows.builder import FieldViolation, WorkflowValidationError
from gretel_client.workflows.configs.tasks import (
    CodeLang,
    ConcatDatasets,
    DropColumns,
    EvaluateDataset,
    GenerateColumnFromTemplateV2,
    GenerateColumnsUsingSamplers,
    JudgeWithLlm,
    Rubric,
    SampleFromDataset,
    SamplerType,
    ValidateCode,
)


class DummyStructuredModel(BaseModel):
    stub: str


def test_build_data_designer_state_using_types():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.add_column(SamplerColumn(name="test_id", type=SamplerType.UUID, params={}))
    dd.add_column(
        LLMCodeColumn(
            name="test_code",
            prompt="Write some zig but call it Python.",
            output_format="python",
        )
    )
    dd.add_column(
        LLMStructuredColumn(
            name="test_structured_output",
            prompt="Generate a structured output",
            output_format=DummyStructuredModel.model_json_schema(),
        )
    )
    dd.add_column(
        LLMJudgeColumn(
            name="test_judge",
            prompt="Judge this",
            rubrics=[
                Rubric(
                    name="test_rubric",
                    description="test",
                    scoring={"0": "Not Good", "1": "Good"},
                )
            ],
        )
    )
    dd.add_column(
        CodeValidationColumn(
            name="test_validation",
            code_lang=CodeLang.PYTHON,
            target_column="test_code",
        )
    )
    assert dd.get_columns_of_type(SamplerColumn)[0].name == "test_id"
    assert dd.get_columns_of_type(LLMCodeColumn)[0].name == "test_code"
    assert (
        dd.get_columns_of_type(LLMStructuredColumn)[0].name == "test_structured_output"
    )
    assert (
        dd.get_columns_of_type(LLMStructuredColumn)[0].output_format
        == DummyStructuredModel.model_json_schema()
    )
    assert dd.get_columns_of_type(LLMJudgeColumn)[0].name == "test_judge"
    assert dd.get_columns_of_type(CodeValidationColumn)[0].name == "test_validation"
    assert len(dd._columns) == 5


def test_data_designer_from_config(stub_aidd_config_str):
    dd = DataDesigner.from_config(
        gretel_resource_provider=MagicMock(), config=stub_aidd_config_str
    )
    assert isinstance(dd.get_column(column_name="code_id"), SamplerColumn)
    assert isinstance(dd.get_column(column_name="text"), LLMTextColumn)
    assert isinstance(dd.get_column(column_name="code"), LLMCodeColumn)
    assert isinstance(
        dd.get_column(column_name="code_validation_result"),
        CodeValidationColumn,
    )
    assert isinstance(dd.get_column(column_name="code_judge_result"), LLMJudgeColumn)
    assert isinstance(dd.get_evaluation_report(), EvaluationReportT)

    assert dd.model_suite == "apache-2.0"
    assert dd.model_configs[0].alias == "my_own_code_model"

    ## Magic checks
    assert dd.get_column(column_name="code_id") in dd.magic.known_columns
    assert dd.get_column(column_name="text") in dd.magic.known_columns
    assert dd.get_column(column_name="code") in dd.magic.known_columns

    assert dd.get_column(column_name="text") in dd.magic.editable_columns
    assert dd.get_column(column_name="code") in dd.magic.editable_columns


def test_column_operations():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    # chain addition of columns via add_column interface
    dd = dd.add_column(
        name="code_id",
        type="uuid",
        params={"prefix": "code_", "short_form": True, "uppercase": True},
    ).add_column(name="text", prompt="Write a description of fizzbuzz python code")
    assert isinstance(dd.get_column("code_id"), SamplerColumn)
    assert isinstance(dd.get_column("text"), LLMTextColumn)

    # replace existing column
    dd = dd.add_column(name="text", prompt="Updated")
    assert dd.get_column(column_name="text").prompt == "Updated"

    # delete column by name
    dd = dd.delete_column("code_id")
    assert len(dd.get_columns_of_type(SamplerColumn)) == 0

    # add validation columns
    dd = dd.add_column(name="code", prompt="generate some python code")
    dd = dd.add_column(
        name="code_validation_result",
        type="code-validation",
        code_lang="python",
        target_column="text",
    )
    assert isinstance(dd.get_column("code_validation_result"), CodeValidationColumn)

    # add judge columns
    dd = dd.add_column(
        name="code_judge_result",
        type="llm-judge",
        prompt="some judge prompt",
        rubrics=[MagicMock(spec=Rubric)],
    )
    assert isinstance(dd.get_column("code_judge_result"), LLMJudgeColumn)

    # add column of invalid type
    with pytest.raises(
        ValueError, match="Invalid column provider type: 'invalid-type'."
    ):
        _ = dd.add_column(name="some_column", type="invalid-type")


def test_constraint_operations():
    dd = (
        DataDesigner(gretel_resource_provider=MagicMock())
        .add_column(name="age", type="gaussian", params={"mean": 35, "stddev": 5})
        .add_column(name="height", type="uniform", params={"low": 15, "high": 200})
        .add_constraint(
            target_column="age",
            type="scalar_inequality",
            params={"operator": "lt", "rhs": 35},
        )
        # add another constraint to replace the old one
        .add_constraint(
            target_column="age",
            type="scalar_inequality",
            params={"operator": "gt", "rhs": 30},
        )
        .add_constraint(
            target_column="height",
            type="column_inequality",
            params={"operator": "gt", "rhs": "age"},
        )
    )
    assert dd.get_constraint(target_column="age").params.operator == "gt"
    assert dd.get_constraint(target_column="age").params.rhs == 30

    # delete constraint by name
    dd.delete_constraint(target_column="height")
    assert dd.get_constraint(target_column="height") is None


def test_evaluation_operations():
    dd = (
        DataDesigner(gretel_resource_provider=MagicMock())
        .add_column(name="text", prompt="Write a description of python code")
        .add_column(name="code", prompt="Write Python code")
        .with_evaluation_report()
    )

    # delete evaluation by type
    dd.delete_evaluation_report()
    assert dd.get_evaluation_report() is None

    # invalid settings
    with pytest.raises(ValueError):
        (
            DataDesigner(gretel_resource_provider=MagicMock())
            .add_column(name="text", prompt="Write a description of python code")
            .add_column(name="code", prompt="Write Python code")
            .with_evaluation_report(settings={"validation_columns": 123})
        )


def test_config_operations(stub_aidd_config_str):
    dd = DataDesigner.from_config(
        gretel_resource_provider=MagicMock(), config=stub_aidd_config_str
    )
    # verify transformation back to AIDDConfig
    aidd_config = dd.config
    assert isinstance(aidd_config, AIDDConfig)
    assert aidd_config.model_suite == dd.model_suite

    # verify transformation to dict
    aidd_config_dict = dd.config.to_dict()
    assert isinstance(aidd_config_dict, dict)
    assert aidd_config_dict["model_suite"] == dd.model_suite

    # verify config export to files
    with tempfile.NamedTemporaryFile(prefix="config", suffix=".json") as tmp_file:
        dd.config.to_json(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            assert json.loads(f.read())["model_suite"] == dd.model_suite

    with tempfile.NamedTemporaryFile(prefix="config", suffix=".yaml") as tmp_file:
        dd.config.to_yaml(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            deserialized_config = yaml.safe_load(f.read())
            assert deserialized_config["model_suite"] == dd.model_suite
            # verify enums are rendered as plain strings in the yaml file
            assert isinstance(deserialized_config["model_suite"], str)

    with tempfile.NamedTemporaryFile(prefix="config", suffix=".yml") as tmp_file:
        dd.config.to_yaml(path=tmp_file.name)
        with open(tmp_file.name, "r") as f:
            assert yaml.safe_load(f.read())["model_suite"] == dd.model_suite


def test_error_on_column_name_same_as_latent_person_sampler():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.with_person_samplers({"dude": {"locale": "en_GB"}})

    # this is an upsert, so no error
    dd.with_person_samplers({"dude": {"locale": "en_GB"}})

    dd.add_column(name="dude_2", type="category", params={"values": ["John", "Jane"]})

    # this is an upsert, so no error
    dd.add_column(name="dude_2", type="category", params={"values": ["Mike", "Jane"]})

    # latent person samplers can't be overwritten with add_column
    with pytest.raises(ValueError, match="already the name of a person sampler."):
        dd.add_column(name="dude", type="category", params={"values": ["John", "Jane"]})


def test_build_workflow_validation_error_handling(
    stub_aidd_config_str, mock_low_level_sdk_resources
):
    stub_field_err_msg = "stub field error message"
    stub_violation = FieldViolation(
        field="stub.field",
        error_message=stub_field_err_msg,
    )
    mock_low_level_sdk_resources.mock_workflow_builder.add_step.side_effect = (
        WorkflowValidationError(
            "Stub error message",
            step_name="stub-step-name",
            task_name="stub_task_name",
            field_violations=[stub_violation],
        )
    )

    dd = DataDesigner.from_config(
        gretel_resource_provider=mock_low_level_sdk_resources.mock_resource_provider,
        config=stub_aidd_config_str,
    )
    with pytest.raises(DataDesignerValidationError) as e:
        _ = dd.preview()
    err_msg = str(e.value)
    assert "error(s) found" in err_msg
    assert stub_field_err_msg in err_msg


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
    assert len(steps[1].data_schema.columns) == 6
    expected_sampling_column_names = set(
        [c.name for c in dd.get_columns_of_type(SamplerColumn)]
    )
    actual_sampling_column_names = set(
        set([col.name for col in steps[1].data_schema.columns])
    )
    assert expected_sampling_column_names == actual_sampling_column_names
    assert len(steps[1].data_schema.constraints) == 1
    assert steps[1].data_schema.constraints[0].target_column == "age"

    assert isinstance(steps[2], ConcatDatasets)
    assert isinstance(steps[3], GenerateColumnFromTemplateV2)
    assert steps[3].name == "text"

    assert isinstance(steps[4], GenerateColumnFromTemplateV2)
    assert steps[4].name == "code"

    assert isinstance(steps[5], JudgeWithLlm)
    assert steps[5].result_column == "code_judge_result"

    assert isinstance(steps[6], ValidateCode)
    assert steps[6].code_lang == "python"

    assert isinstance(steps[7], DropColumns)
    assert steps[7].columns == ["some_dude", "some_lady"]

    assert isinstance(steps[8], EvaluateDataset)

    # TODO: Add more assertions for validators and evaluators


def test_workflow_builder_run_integration(
    stub_aidd_config_str, mock_low_level_sdk_resources
):
    dd = DataDesigner.from_config(
        gretel_resource_provider=mock_low_level_sdk_resources.mock_resource_provider,
        config=stub_aidd_config_str,
    )
    dd.create(
        num_records=1000,
        name="test-wfl",
        wait_until_done=True,
    )
    mock_low_level_sdk_resources.mock_workflow_builder.run.assert_called_with(
        name="test-wfl", wait_until_done=True
    )


def test_get_column_from_kwargs():
    # Test column creation and serialization

    # Test LLM_TEXT column
    llm_text_column = get_column_from_kwargs(
        name="test_llm_text", type=ProviderType.LLM_TEXT, prompt="Write some text"
    )
    assert isinstance(llm_text_column, LLMTextColumn)
    assert llm_text_column.name == "test_llm_text"
    assert llm_text_column.prompt == "Write some text"
    assert llm_text_column.model_alias == "text"
    assert llm_text_column.output_type == "text"
    assert llm_text_column.model_dump()["output_type"] == "text"

    # Test LLM_CODE column
    llm_code_column = get_column_from_kwargs(
        name="test_llm_code",
        type=ProviderType.LLM_CODE,
        prompt="Write some code",
        output_format="python",
    )
    assert isinstance(llm_code_column, LLMCodeColumn)
    assert llm_code_column.name == "test_llm_code"
    assert llm_code_column.prompt == "Write some code"
    assert llm_code_column.model_alias == "code"
    assert llm_code_column.output_type == "code"
    assert llm_code_column.output_format == "python"
    assert llm_code_column.model_dump()["output_type"] == "code"

    # Test LLM_STRUCTURED column
    llm_structured_column = get_column_from_kwargs(
        name="test_llm_structured",
        type=ProviderType.LLM_STRUCTURED,
        prompt="Generate a structured output",
        output_format=DummyStructuredModel.model_json_schema(),
    )
    assert isinstance(llm_structured_column, LLMStructuredColumn)
    assert llm_structured_column.name == "test_llm_structured"
    assert llm_structured_column.prompt == "Generate a structured output"
    assert (
        llm_structured_column.output_format == DummyStructuredModel.model_json_schema()
    )
    assert llm_structured_column.model_alias == "structured"
    assert llm_structured_column.output_type == "structured"
    assert llm_structured_column.model_dump()["output_type"] == "structured"

    # Test LLM_JUDGE column
    llm_judge_column = get_column_from_kwargs(
        name="test_judge",
        type=ProviderType.LLM_JUDGE,
        prompt="Judge this code",
        rubrics=[
            Rubric(
                name="test_rubric",
                description="test",
                scoring={"0": "Bad", "1": "Good"},
            )
        ],
    )
    assert isinstance(llm_judge_column, LLMJudgeColumn)
    assert llm_judge_column.name == "test_judge"
    assert llm_judge_column.prompt == "Judge this code"
    assert len(llm_judge_column.rubrics) == 1

    # Test CODE_VALIDATION column
    code_validation_column = get_column_from_kwargs(
        name="test_validation",
        type=ProviderType.CODE_VALIDATION,
        code_lang=CodeLang.PYTHON,
        target_column="test_code",
    )
    assert isinstance(code_validation_column, CodeValidationColumn)
    assert code_validation_column.name == "test_validation"
    assert code_validation_column.code_lang == CodeLang.PYTHON
    assert code_validation_column.target_column == "test_code"

    # Test EXPRESSION column
    expression_column = get_column_from_kwargs(
        name="test_expression", type=ProviderType.EXPRESSION, expr="1 + 1"
    )
    assert isinstance(expression_column, ExpressionColumn)
    assert expression_column.name == "test_expression"
    assert expression_column.expr == "1 + 1"

    # Test Sampler columns with nullable params
    # UUID type with params provided
    sampler_column = get_column_from_kwargs(
        name="test_sampler",
        type=SamplerType.UUID,
        params={"prefix": "test_", "short_form": True},
    )
    assert isinstance(sampler_column, SamplerColumn)
    assert sampler_column.name == "test_sampler"
    assert sampler_column.type == SamplerType.UUID
    assert sampler_column.params.prefix == "test_"
    assert sampler_column.params.short_form is True
    assert sampler_column.params.uppercase is False

    # UUID type without params provided
    sampler_column_no_params = get_column_from_kwargs(
        name="test_sampler_no_params", type=SamplerType.UUID
    )
    assert isinstance(sampler_column_no_params, SamplerColumn)
    assert sampler_column_no_params.name == "test_sampler_no_params"
    assert sampler_column_no_params.type == SamplerType.UUID
    assert sampler_column_no_params.params.prefix is None
    assert sampler_column_no_params.params.short_form is False
    assert sampler_column_no_params.params.uppercase is False

    # PERSON type with params provided
    person_sampler_column = get_column_from_kwargs(
        name="test_person_sampler",
        type=SamplerType.PERSON,
        params={
            "locale": "en_BR",
            "sex": "Male",
            "city": "New York",
            "age_range": [18, 30],
        },
    )
    assert isinstance(person_sampler_column, SamplerColumn)
    assert person_sampler_column.name == "test_person_sampler"
    assert person_sampler_column.type == SamplerType.PERSON
    assert person_sampler_column.params.locale == "en_BR"
    assert person_sampler_column.params.sex == "Male"
    assert person_sampler_column.params.city == "New York"

    # PersonSampler type without params provided
    person_sampler_column_no_params = get_column_from_kwargs(
        name="test_person_sampler_no_params", type=SamplerType.PERSON
    )
    assert isinstance(person_sampler_column_no_params, SamplerColumn)
    assert person_sampler_column_no_params.name == "test_person_sampler_no_params"
    assert person_sampler_column_no_params.type == SamplerType.PERSON
    assert person_sampler_column_no_params.params.locale == "en_US"
    assert person_sampler_column_no_params.params.sex is None
    assert person_sampler_column_no_params.params.city is None
