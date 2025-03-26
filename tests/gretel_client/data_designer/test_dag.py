from unittest.mock import MagicMock

import pytest

import gretel_client.data_designer.columns as C
import gretel_client.data_designer.params as P

from gretel_client.data_designer import DataDesigner
from gretel_client.data_designer.dag import topologically_sort_columns


def test_dag_construction():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.add_column(
        C.SamplerColumn(name="test_id", type=P.SamplingSourceType.UUID, params={})
    )
    dd.add_column(
        C.LLMGenColumn(
            name="test_code",
            prompt="Write some zig but call it Python.",
            model_alias="code",
            data_config=P.DataConfig(
                type=P.OutputType.CODE, params={"syntax": P.CodeLang.PYTHON}
            ),
        )
    )
    dd.add_column(
        C.LLMGenColumn(
            name="depends_on_validation",
            prompt="Write {{ test_code_pylint_score }}.",
            model_alias="code",
            data_config=P.DataConfig(
                type=P.OutputType.CODE, params={"syntax": P.CodeLang.PYTHON}
            ),
        )
    )
    dd.add_column(
        C.LLMJudgeColumn(
            name="test_judge",
            prompt="Judge this {{ test_code }} {{ depends_on_validation }}",
            rubrics=[
                P.Rubric(
                    name="test_rubric",
                    description="test",
                    scoring={"0": "Not Good", "1": "Good"},
                )
            ],
        )
    )
    dd.add_column(
        C.CodeValidationColumn(
            name="test_validation",
            code_lang=P.CodeLang.PYTHON,
            target_column="test_code",
        )
    )
    sorted_column_names = topologically_sort_columns(dd._dag_columns)
    assert sorted_column_names == [
        "test_code",
        "test_validation",
        "depends_on_validation",
        "test_judge",
    ]


def test_circular_dependencies():
    dd = DataDesigner(gretel_resource_provider=MagicMock())
    dd.add_column(
        C.SamplerColumn(name="test_id", type=P.SamplingSourceType.UUID, params={})
    )
    dd.add_column(
        C.LLMGenColumn(
            name="col_1",
            prompt="I need you {{ col_2 }}",
        )
    )
    dd.add_column(
        C.LLMGenColumn(
            name="col_2",
            prompt="I need you {{ col_1 }}",
        )
    )
    with pytest.raises(ValueError, match="cyclic dependencies"):
        topologically_sort_columns(dd._dag_columns)
