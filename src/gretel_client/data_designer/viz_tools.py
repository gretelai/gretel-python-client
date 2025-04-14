import json
import numbers

from typing import Self, TYPE_CHECKING

import numpy as np
import pandas as pd

from pydantic import BaseModel
from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.pretty import Pretty
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from gretel_client.data_designer.constants import (
    DEFAULT_HIST_NAME_COLOR,
    DEFAULT_HIST_VALUE_COLOR,
)
from gretel_client.data_designer.judge_rubrics import JudgeRubric
from gretel_client.data_designer.types import EvaluationType, LLMJudgePromptTemplateType
from gretel_client.data_designer.utils import code_lang_to_syntax_lexer
from gretel_client.workflows.configs.tasks import CodeLang, OutputType

if TYPE_CHECKING:
    from gretel_client.data_designer.data_designer import DataDesigner


console = Console()


class AIDDMetadata(BaseModel):
    """Metadata related to the dataset created by DataDesigner.

    We pass this object around to enable streamlined helper methods like
    `display_sample_record`, `fetch_dataset`, and `download_evaluation_report`.
    """

    sampler_columns: list[str] = []
    seed_columns: list[str] = []
    llm_text_columns: list[str] = []
    llm_code_columns: list[str] = []
    llm_structured_columns: list[str] = []
    llm_judge_columns: list[str] = []
    validation_columns: list[str] = []
    expression_columns: list[str] = []
    evaluation_columns: list[str] = []
    person_samplers: list[str] = []
    code_langs: list[CodeLang | str] = []
    eval_type: LLMJudgePromptTemplateType | None = None

    @classmethod
    def from_aidd(cls, aidd: "DataDesigner") -> Self:
        code_validation_columns = []

        for code_val_col in aidd.code_validation_columns:
            code_validation_columns.extend(
                [code_val_col.name] + list(code_val_col.side_effect_columns)
            )

        sampling_based_columns = [
            col.name
            for col in aidd.sampler_columns
            if col.name not in list(aidd._latent_person_columns.keys())
        ]

        # Temporary logic to funnel LLMGenColumn column names into the correct list.
        # This can be removed once we migrate magic to the new column types.
        llm_text_columns = []
        llm_code_columns = []
        llm_structured_columns = []
        for col in aidd.llm_gen_columns:
            if col.output_type == OutputType.TEXT and col.name not in [
                c.name for c in aidd.llm_text_columns
            ]:
                llm_text_columns.append(col)
            elif col.output_type == OutputType.CODE and col.name not in [
                c.name for c in aidd.llm_code_columns
            ]:
                llm_code_columns.append(col)
            elif col.output_type == OutputType.STRUCTURED and col.name not in [
                c.name for c in aidd.llm_structured_columns
            ]:
                llm_structured_columns.append(col)
        llm_text_columns = aidd.llm_text_columns + llm_text_columns
        llm_code_columns = aidd.llm_code_columns + llm_code_columns
        llm_structured_columns = aidd.llm_structured_columns + llm_structured_columns

        return cls(
            sampler_columns=sampling_based_columns,
            seed_columns=[col.name for col in aidd.seed_columns],
            llm_text_columns=[col.name for col in llm_text_columns],
            llm_code_columns=[col.name for col in llm_code_columns],
            llm_structured_columns=[col.name for col in llm_structured_columns],
            llm_judge_columns=[col.name for col in aidd.llm_judge_columns],
            validation_columns=code_validation_columns,
            expression_columns=[col.name for col in aidd.expression_columns],
            person_samplers=list(aidd._latent_person_columns.keys()),
            code_langs=[col.output_format for col in aidd.llm_code_columns],
            eval_type=None,
        )


def create_rich_histogram_table(
    data: dict[str, int | float],
    column_names: tuple[int, int],
    title: str | None = None,
    name_color: str = DEFAULT_HIST_NAME_COLOR,
    value_color: str = DEFAULT_HIST_VALUE_COLOR,
) -> Table:
    table = Table(title=title, title_style="bold")
    table.add_column(column_names[0], justify="right", style=name_color)
    table.add_column(column_names[1], justify="left", style=value_color)

    max_count = max(data.values())

    for name, value in data.items():
        bar = "" if max_count <= 0 else "█" * int((value / max_count) * 20)
        table.add_row(str(name), f"{bar} {value:.1f}")

    return table


def display_sample_record(
    record: dict | pd.Series | pd.DataFrame,
    aidd_metadata: AIDDMetadata,
    background_color: str | None = None,
    syntax_highlighting_theme: str = "dracula",
    record_index: int | None = None,
    hide_seed_columns: bool = False,
):
    if isinstance(record, (dict, pd.Series)):
        record = pd.DataFrame([record]).iloc[0]
    elif isinstance(record, pd.DataFrame):
        if record.shape[0] > 1:
            raise ValueError(
                "The record must be a single record. You provided a "
                f"DataFrame with {record.shape[0]} records."
            )
        record = record.iloc[0]
    else:
        raise ValueError(
            "The record must be a single record in a dictionary, pandas Series, "
            f"or pandas DataFrame. You provided: {type(record)}."
        )

    table_kws = dict(show_lines=True, expand=True)

    render_list = []

    if not hide_seed_columns and len(aidd_metadata.seed_columns) > 0:
        table = Table(title="Seed Columns", **table_kws)
        table.add_column("Name")
        table.add_column("Value")
        for col in aidd_metadata.seed_columns:
            table.add_row(col, _convert_to_row_element(record[col]))
        render_list.append(_pad_console_element(table))

    non_code_columns = (
        aidd_metadata.sampler_columns
        + aidd_metadata.expression_columns
        + aidd_metadata.llm_text_columns
        + aidd_metadata.llm_structured_columns
    )

    if len(non_code_columns) > 0:
        table = Table(title="Generated Columns", **table_kws)
        table.add_column("Name")
        table.add_column("Value")
        for col in [c for c in non_code_columns]:
            table.add_row(col, _convert_to_row_element(record[col]))
        render_list.append(_pad_console_element(table))

    for num, col in enumerate(aidd_metadata.llm_code_columns):
        if not aidd_metadata.code_langs:
            raise ValueError(
                "`code_langs` must be provided when code columns are specified."
            )
        code_lang = aidd_metadata.code_langs[num]
        if code_lang is None:
            raise ValueError(
                "`code_lang` must be provided when code columns are specified."
                f"Valid options are: {', '.join([c.value for c in CodeLang])}"
            )
        panel = Panel(
            Syntax(
                record[col],
                lexer=code_lang_to_syntax_lexer(code_lang),
                theme=syntax_highlighting_theme,
                word_wrap=True,
                background_color=background_color,
            ),
            title=col,
            expand=True,
        )
        render_list.append(_pad_console_element(panel))

    if len(aidd_metadata.validation_columns) > 0:
        table = Table(title="Validation", **table_kws)
        row = []
        for col in aidd_metadata.validation_columns:
            value = record[col]
            if isinstance(value, numbers.Number):
                table.add_column(col)
                row.append(f"{value:.2f}")
            elif isinstance(value, (list, tuple, np.ndarray)) and len(value) > 0:
                length = len(value)
                label = "" if length == 1 else f" (first of {length} messages)"
                table.add_column(f"{col}{label}")
                row.append(str(value[0]))
            else:
                table.add_column(col)
                row.append(str(value))
        table.add_row(*row)
        render_list.append(_pad_console_element(table, (1, 0, 1, 0)))

    if len(aidd_metadata.llm_judge_columns) > 0:
        for col in aidd_metadata.llm_judge_columns:
            table = Table(title=f"LLM-as-a-Judge: {col}", **table_kws)
            row = []
            judge = record[col]

            for measure, results in judge.items():
                table.add_column(measure)
                row.append(
                    f"score: {results['score']}\n" f"reasoning: {results['reasoning']}"
                )
            table.add_row(*row)
            render_list.append(_pad_console_element(table, (1, 0, 1, 0)))

    if record_index is not None:
        index_label = Text(f"[index: {record_index}]", justify="center")
        render_list.append(index_label)

    console.print(Group(*render_list), markup=False)


def display_preview_evaluation_summary(
    eval_type: EvaluationType | LLMJudgePromptTemplateType,
    eval_results: dict,
    hist_name_color: str = DEFAULT_HIST_NAME_COLOR,
    hist_value_color: str = DEFAULT_HIST_VALUE_COLOR,
):

    render_list = []

    dash_sep = Text("-" * 100, style="bold")
    viz_name = Text(" " * 32 + "📊 Preview Evaluation Summary 📊", style="bold")

    render_list.append(dash_sep)
    render_list.append(viz_name)
    render_list.append(dash_sep)

    metrics = {}

    results = eval_results["results"]
    if "valid_records_score" in results:
        metrics["Valid Code"] = results["valid_records_score"]["percent"] * 100
    metrics.update(
        {
            "Completely Unique": results["row_uniqueness"]["percent_unique"],
            "Semantically Unique": results["row_uniqueness"][
                "percent_semantically_unique"
            ],
        }
    )

    console_columns = []
    metrics_hist = create_rich_histogram_table(
        metrics,
        (
            "Values",
            "Percent of Records",
        ),
        "Quality & Diversity" if len(metrics) == 3 else "Diversity",
        name_color=hist_name_color,
        value_color=hist_value_color,
    )
    metrics_hist = Align(metrics_hist, vertical="bottom", align="left")
    console_columns.append(metrics_hist)

    if eval_type in list(LLMJudgePromptTemplateType):
        rubric = JudgeRubric.get_rubric(eval_type)
        judge_summary = {}
        for k in rubric.keys():
            judge_summary[k.capitalize()] = results.get(
                "llm_as_a_judge_mean_scores", {}
            ).setdefault(f"{k}_score", 0)
        judge_hist = create_rich_histogram_table(
            judge_summary,
            column_names=("Rubric", "Mean Score (0 - 5)"),
            title="LLM-as-a-Judge",
            name_color=hist_name_color,
            value_color=hist_value_color,
        )
        judge_hist = Align(judge_hist, vertical="bottom", align="right")
        console_columns.append(judge_hist)

    console_columns = Columns(console_columns, padding=(0, 7))
    render_list.append(_pad_console_element(console_columns, (1, 0, 0, 0)))

    fields = ["average_words_per_record", "average_tokens_per_record", "total_tokens"]
    text_stats = results.get("num_words_per_record")
    if text_stats is not None:
        text_stats_table = Table(
            expand=True, title="Text Stats", width=100, title_style="bold"
        )
        for field in fields:
            text_stats_table.add_column(field, justify="right")
        text_stats_table.add_row(
            *[
                (
                    f"{text_stats[field]:.1f}"
                    if isinstance(text_stats[field], float)
                    else str(text_stats[field])
                )
                for field in fields
            ]
        )
        render_list.append(_pad_console_element(text_stats_table, (2, 0, 1, 0)))

    render_list.append(dash_sep)

    console.print(Group(*render_list), markup=False)


def _convert_to_row_element(elem):
    try:
        elem = Pretty(json.loads(elem))
    except (TypeError, json.JSONDecodeError):
        pass
    if isinstance(elem, (np.integer, np.floating, np.ndarray)):
        elem = str(elem)
    elif isinstance(elem, (list, dict)):
        elem = Pretty(elem)
    return elem


def _pad_console_element(elem, padding=(1, 0, 1, 0)):
    return Padding(elem, padding)
