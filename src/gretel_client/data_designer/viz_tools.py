import json
import numbers

from typing import Optional, Union

import numpy as np
import pandas as pd

from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.pretty import Pretty
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from gretel_client.data_designer.judge_rubrics import JudgeRubric
from gretel_client.data_designer.types import EvaluationType, LLMJudgePromptTemplateType
from gretel_client.data_designer.utils import code_lang_to_syntax_lexer
from gretel_client.workflows.configs.tasks import CodeLang

console = Console()

DEFAULT_HIST_NAME_COLOR = "medium_purple1"
DEFAULT_HIST_VALUE_COLOR = "pale_green3"


def _pad_console_element(elem, padding=(1, 0, 1, 0)):
    return Padding(elem, padding)


def create_rich_histogram_table(
    data: dict[str, Union[int, float]],
    column_names: tuple[int, int],
    title: Optional[str] = None,
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
    record: Union[dict, pd.Series, pd.DataFrame],
    sampling_based_columns: Optional[list[str]],
    prompt_based_columns: Optional[list[str]],
    llm_judge_columns: Optional[list[str]] = None,
    code_lang: Optional[CodeLang] = None,
    code_columns: Optional[list[str]] = None,
    validation_columns: Optional[list[str]] = None,
    background_color: Optional[str] = None,
    syntax_highlighting_theme: str = "dracula",
    record_index: Optional[int] = None,
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

    code_columns = code_columns or []
    sampling_based_columns = sampling_based_columns or []
    prompt_based_columns = prompt_based_columns or []
    validation_columns = validation_columns or []

    table_kws = dict(show_lines=True, expand=True)

    render_list = []

    columns_combined = sampling_based_columns + prompt_based_columns
    if len(columns_combined) > 0:
        table = Table(title="Generated Columns", **table_kws)
        table.add_column("Name")
        table.add_column("Value")
        for col in [c for c in columns_combined if c not in code_columns]:
            ## Pretty-print for structured outputs
            _element = record[col]
            try:
                _element = Pretty(json.loads(_element))
            except (TypeError, json.JSONDecodeError):
                pass
            if isinstance(_element, (np.integer, np.floating, np.ndarray)):
                _element = str(_element)
            elif isinstance(_element, (list, dict)):
                _element = Pretty(_element)
            table.add_row(col, _element)
        render_list.append(_pad_console_element(table))

    for col in code_columns:
        if code_lang is None:
            raise ValueError(
                "`code_lang` must be provided when code_columns are specified."
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

    if len(validation_columns) > 0:
        table = Table(title="Validation", **table_kws)
        row = []
        for col in validation_columns:
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

    if len(llm_judge_columns) > 0:

        for col in llm_judge_columns:
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
    eval_type: Union[EvaluationType, LLMJudgePromptTemplateType],
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
