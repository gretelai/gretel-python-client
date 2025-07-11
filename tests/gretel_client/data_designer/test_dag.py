from unittest.mock import MagicMock

import pytest

from gretel_client.data_designer.dag import topologically_sort_columns
from gretel_client.data_designer.data_designer import DataDesigner
from gretel_client.data_designer.types import (
    CodeValidationColumn,
    ExpressionColumn,
    LLMCodeColumn,
    LLMJudgeColumn,
    LLMTextColumn,
    SamplerColumn,
)
from gretel_client.workflows.configs.tasks import CodeLang, Rubric, SamplerType


def test_dag_construction():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.add_column(SamplerColumn(name="test_id", type=SamplerType.UUID, params={}))
    dd.add_column(
        LLMCodeColumn(
            name="test_code",
            prompt="Write some zig but call it Python.",
            output_format=CodeLang.PYTHON,
        )
    )
    dd.add_column(
        LLMCodeColumn(
            name="depends_on_validation",
            prompt="Write {{ test_code_python_linter_score }}.",
            output_format=CodeLang.PYTHON,
        )
    )
    dd.add_column(
        LLMJudgeColumn(
            name="test_judge",
            prompt="Judge this {{ test_code }} {{ depends_on_validation }}",
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
        ExpressionColumn(
            name="uses_all_the_stuff",
            expr="{{ test_code }} {{ depends_on_validation }} {{ test_judge }}",
        )
    )
    dd.add_column(
        CodeValidationColumn(
            name="test_validation",
            code_lang=CodeLang.PYTHON,
            target_column="test_code",
        )
    )
    sorted_column_names = topologically_sort_columns(dd._dag_columns)
    assert sorted_column_names == [
        "test_code",
        "test_validation",
        "depends_on_validation",
        "test_judge",
        "uses_all_the_stuff",
    ]


def test_circular_dependencies():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.add_column(SamplerColumn(name="test_id", type=SamplerType.UUID, params={}))
    dd.add_column(
        LLMTextColumn(
            name="col_1",
            prompt="I need you {{ col_2 }}",
        )
    )
    dd.add_column(
        LLMTextColumn(
            name="col_2",
            prompt="I need you {{ col_1 }}",
        )
    )
    with pytest.raises(ValueError, match="cyclic dependencies"):
        topologically_sort_columns(dd._dag_columns)
