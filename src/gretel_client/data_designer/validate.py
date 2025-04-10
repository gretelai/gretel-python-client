from enum import Enum
from string import Formatter
from typing import Literal

from jinja2 import meta
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import BaseModel
from rich import box
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel

from gretel_client.data_designer.constants import RICH_CONSOLE_THEME
from gretel_client.data_designer.types import (
    AIDDColumnT,
    CodeValidationColumn,
    LLMGenColumn,
    LLMJudgeColumn,
)
from gretel_client.workflows.configs.tasks import CodeLang, OutputType


class ViolationType(str, Enum):
    INVALID_REFERENCE = "invalid_reference"
    F_STRING_SYNTAX = "f_string_syntax"
    CODE_COLUMN_MISSING = "code_column_missing"
    CODE_COLUMN_NOT_CODE = "code_column_not_code"
    CODE_LANG_MISMATCH = "code_lang_mismatch"
    PROMPT_WITHOUT_REFERENCES = "prompt_without_references"


class Violation(BaseModel):
    column: str
    type: ViolationType
    message: str
    level: Literal["ERROR", "WARNING"]


def validate_aidd_columns(
    columns: list[AIDDColumnT], allowed_references: list[str]
) -> list[Violation]:
    violations = []
    violations.extend(
        _validate_prompt_templates(
            columns=columns,
            allowed_references=allowed_references,
        )
    )
    violations.extend(
        _validate_code_validation(
            columns=columns,
        )
    )
    return violations


def rich_print_violations(violations: list[Violation]) -> None:
    if len(violations) == 0:
        return

    console = Console(theme=RICH_CONSOLE_THEME)

    render_list = []
    render_list.append(
        Padding(
            Panel(
                f"🔎 Identified {len(violations)} validation "
                f"issue{'' if len(violations) == 1 else 's'} "
                "in your Data Designer column definitions",
                box=box.SIMPLE,
                highlight=True,
            ),
            (0, 0, 1, 0),
        )
    )

    for v in violations:
        emoji = "🛑" if v.level == "ERROR" else "⚠️"

        error_title = f"{emoji} {v.level.upper()} | {v.type.value.upper()}"

        render_list.append(
            Padding(
                Panel(
                    f"{error_title}\n\n{v.message}",
                    box=box.HORIZONTALS,
                    title=f"Column: {v.column}",
                    padding=(1, 0, 1, 1),
                    highlight=True,
                ),
                (0, 0, 1, 0),
            )
        )

    console.print(Group(*render_list), markup=False)


def _get_string_formatter_references(
    template: str, allowed_references: list[str]
) -> list[str]:
    return [
        k[1].strip()
        for k in Formatter().parse(template)
        if len(k) > 1 and k[1] is not None and k[1].strip() in allowed_references
    ]


def _validate_prompt_templates(
    columns: AIDDColumnT,
    allowed_references: list[str],
) -> list[Violation]:
    env = ImmutableSandboxedEnvironment()

    columns_with_prompts = [
        c for c in columns if isinstance(c, (LLMGenColumn, LLMJudgeColumn))
    ]

    violations = []
    for column in columns_with_prompts:
        for prompt_type in ["prompt", "system_prompt"]:
            if not hasattr(column, prompt_type) or getattr(column, prompt_type) is None:
                continue

            prompt = getattr(column, prompt_type)

            # check for invalid references
            prompt_references = set()
            prompt_references.update(meta.find_undeclared_variables(env.parse(prompt)))
            invalid_references = list(set(prompt_references) - set(allowed_references))
            num_invalid = len(invalid_references)
            if num_invalid > 0:
                ref_msg = (
                    f"references {num_invalid} columns that do not exist"
                    if num_invalid > 1
                    else "references a column that does not exist"
                )
                invalid_references = ", ".join([f"'{r}'" for r in invalid_references])
                message = (
                    f"The {prompt_type} template for '{column.name}' "
                    f"{ref_msg}: {invalid_references}."
                )
                violations.append(
                    Violation(
                        column=column.name,
                        type=ViolationType.INVALID_REFERENCE,
                        message=message,
                        level="ERROR",
                    )
                )

            # check for prompts without references
            if prompt_type == "prompt" and len(prompt_references) == 0:
                message = (
                    f"The {prompt_type} template for '{column.name}' does not reference any columns. "
                    "This means the same prompt will be used for every row in the dataset. To increase "
                    "the diversity of the generated data, consider adding references to other columns "
                    "in the prompt template."
                )
                violations.append(
                    Violation(
                        column=column.name,
                        type=ViolationType.PROMPT_WITHOUT_REFERENCES,
                        message=message,
                        level="WARNING",
                    )
                )

            # check for f-string syntax
            f_string_references = _get_string_formatter_references(
                prompt, allowed_references
            )
            if len(f_string_references) > 0:
                f_string_references = ", ".join([f"'{r}'" for r in f_string_references])
                message = (
                    f"The {prompt_type} template for '{column.name}' references the "
                    f"following columns using f-string syntax: {f_string_references}. "
                    "Please use jinja2 syntax to reference columns: {reference} -> {{ reference }}."
                )
                violations.append(
                    Violation(
                        column=column.name,
                        type=ViolationType.F_STRING_SYNTAX,
                        message=message,
                        level="WARNING",
                    )
                )
    return violations


def _validate_code_validation(
    columns: dict[str, AIDDColumnT],
) -> list[Violation]:

    code_validation_columns = [
        c for c in columns if isinstance(c, CodeValidationColumn)
    ]
    columns_by_name = {c.name: c for c in columns}

    violations = []
    for validation_column in code_validation_columns:

        # check that the target column exists
        if validation_column.target_column not in columns_by_name:
            message = f"Target code column '{validation_column.target_column}' not found in column list."
            violations.append(
                Violation(
                    column=validation_column.name,
                    type=ViolationType.CODE_COLUMN_MISSING,
                    message=message,
                    level="ERROR",
                )
            )
            continue

        # check for consistent code languages
        target_column = columns_by_name[validation_column.target_column]
        if isinstance(target_column, LLMGenColumn):
            if target_column.output_type != "code":
                message = (
                    f"Code validation column '{validation_column.name}' is set to validate "
                    f"code, but the target column was generated as {target_column.output_type}."
                )
                violations.append(
                    Violation(
                        column=validation_column.name,
                        type=ViolationType.CODE_COLUMN_NOT_CODE,
                        message=message,
                        level="ERROR",
                    )
                )
            elif target_column.output_format != validation_column.code_lang.value:
                message = (
                    f"Code validation column '{validation_column.name}' is set to validate "
                    f"{validation_column.code_lang.value}, but the target column was generated as "
                    f"{target_column.output_format}."
                )
                violations.append(
                    Violation(
                        column=validation_column.name,
                        type=ViolationType.CODE_LANG_MISMATCH,
                        message=message,
                        level="ERROR",
                    )
                )

    return violations
