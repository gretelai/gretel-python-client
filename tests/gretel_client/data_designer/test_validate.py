from gretel_client.data_designer.judge_rubrics import PythonicRubric
from gretel_client.data_designer.types import (
    CodeValidationColumn,
    LLMGenColumn,
    LLMJudgeColumn,
    SamplerColumn,
)
from gretel_client.data_designer.validate import (
    _validate_code_validation,
    _validate_prompt_templates,
    ViolationType,
)
from gretel_client.workflows.configs.tasks import CodeLang, OutputType

VALID_COLUMNS = [
    SamplerColumn(
        name="random_number",
        type="uniform",
        params={"low": 0, "high": 10},
    ),
    LLMGenColumn(
        name="valid_reference",
        prompt="Why is {{ random_number }} your favorite number?",
    ),
    LLMGenColumn(
        name="code_column_python",
        output_type="code",
        prompt="Generate some python about {{ valid_reference }}.",
        output_format="python",
    ),
]

INVALID_COLUMNS = [
    LLMGenColumn(
        name="text_no_references",
        prompt="Generate a name for the person",
    ),
    LLMGenColumn(
        name="text_invalid_reference",
        prompt="Generate a name for the person: {{ this_column_does_not_exist }}",
    ),
    LLMJudgeColumn(
        name="judge_no_references",
        prompt="Judge the name for the person.",
        rubrics=[PythonicRubric],
    ),
    LLMJudgeColumn(
        name="judge_invalid_reference",
        prompt="Judge the name for the person: {{ this_column_does_not_exist }}",
        rubrics=[PythonicRubric],
    ),
    CodeValidationColumn(
        name="code_validation_python",
        code_lang=CodeLang.SQL_ANSI,
        target_column="code_column_missing",
    ),
    CodeValidationColumn(
        name="code_validation_ansi",
        code_lang=CodeLang.SQL_ANSI,
        target_column="code_column_python",
    ),
    CodeValidationColumn(
        name="code_validation_not_code",
        code_lang=CodeLang.PYTHON,
        target_column="text_no_references",
    ),
]

COLUMNS_WITH_WARNINGS = [
    LLMGenColumn(
        name="unsupported_code_lang",
        output_type=OutputType.CODE,
        prompt="Generate some code for me.",
        output_format="some_new_lang",
    ),
]

COLUMNS = VALID_COLUMNS + INVALID_COLUMNS

ALLOWED_REFERENCE = [c.name for c in COLUMNS]


def test_validate_prompt_templates():
    violations = _validate_prompt_templates(COLUMNS, ALLOWED_REFERENCE)
    assert len(violations) == 4
    assert violations[0].type == ViolationType.PROMPT_WITHOUT_REFERENCES
    assert violations[1].type == ViolationType.INVALID_REFERENCE
    assert violations[2].type == ViolationType.PROMPT_WITHOUT_REFERENCES
    assert violations[3].type == ViolationType.INVALID_REFERENCE


def test_validate_code_validation():
    violations = _validate_code_validation(COLUMNS)
    assert len(violations) == 3
    assert violations[0].type == ViolationType.CODE_COLUMN_MISSING
    assert violations[1].type == ViolationType.CODE_LANG_MISMATCH
    assert violations[2].type == ViolationType.CODE_COLUMN_NOT_CODE


def test_validate_detect_f_string_syntax():
    columns = VALID_COLUMNS
    columns.append(
        LLMGenColumn(
            name="f_string_ref",
            prompt="Why is {random_number} your favorite number? {{ valid_reference }}",
        )
    )
    violations = _validate_prompt_templates(columns, [c.name for c in columns])
    assert len(violations) == 1
    assert violations[0].type == ViolationType.F_STRING_SYNTAX
    assert violations[0].level == "WARNING"
