import inspect
import json
import re

from datetime import date
from pathlib import Path
from typing import Any, Type

import pandas as pd
import requests

from jinja2 import meta
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import BaseModel
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer
from rich.pretty import Pretty

from gretel_client.data_designer.constants import (
    DEFAULT_REPR_HTML_STYLE,
    REPR_HTML_TEMPLATE,
    TASK_TYPE_EMOJI_MAP,
)
from gretel_client.workflows.configs import tasks


def fetch_config_if_remote(config: Any) -> str:
    is_remote = isinstance(config, str) and (
        config.startswith("https://gretel")
        or config.startswith("https://raw.githubusercontent.com/gretelai")
    )
    if is_remote:
        config = requests.get(config).content.decode("utf-8")
    return config


def get_task_log_emoji(task_name: str) -> str:
    log_emoji = ""
    for task_type, emoji in TASK_TYPE_EMOJI_MAP.items():
        if task_name.startswith(task_type):
            log_emoji = emoji + " "
    return log_emoji


def camel_to_kebab(s):
    return re.sub(r"(?<!^)(?=[A-Z])", "-", s).lower()


def make_date_obj_serializable(obj: dict) -> dict:

    class DateTimeEncoder(json.JSONEncoder):

        def default(self, obj: Any) -> Any:
            if isinstance(obj, date):
                return obj.isoformat()
            return super().default(obj)

    return json.loads(json.dumps(obj, cls=DateTimeEncoder))


def code_lang_to_syntax_lexer(code_lang: tasks.CodeLang) -> str:
    """Convert the code language to a syntax lexer for Pygments.

    Reference: https://pygments.org/docs/lexers/
    """
    match code_lang:
        case tasks.CodeLang.PYTHON:
            return "python"
        case tasks.CodeLang.SQLITE:
            return "sql"
        case tasks.CodeLang.ANSI:
            return "sql"
        case tasks.CodeLang.TSQL:
            return "tsql"
        case tasks.CodeLang.BIGQUERY:
            return "sql"
        case tasks.CodeLang.MYSQL:
            return "mysql"
        case tasks.CodeLang.POSTGRES:
            return "postgres"
        case _:
            raise ValueError(f"Unsupported code language: {code_lang}")


def get_prompt_template_keywords(template: str) -> set[str]:
    """Extract all keywords from a string template."""
    ast = ImmutableSandboxedEnvironment().parse(template)
    return set(meta.find_undeclared_variables(ast))


def get_sampler_params() -> dict[str, Type[BaseModel]]:
    """Returns a dictionary of sampler parameter classes."""
    params_cls_list = [
        params_cls
        for _, params_cls in inspect.getmembers(tasks, inspect.isclass)
        if issubclass(
            params_cls, tasks.ConditionalDataColumn.model_fields["params"].annotation
        )
    ]

    params_cls_dict = {}
    for source in tasks.SamplingSourceType:
        # TODO: Update DistributionSamplerParams to ScipySamplerParams
        source_name = source.value if source.value != "scipy" else "distribution"
        params_cls_dict[source.value] = [
            params_cls
            for params_cls in params_cls_list
            if source_name in params_cls.__name__.lower()
        ][0]

    return params_cls_dict


def smart_load_dataframe(dataframe: str | Path | pd.DataFrame) -> pd.DataFrame:
    """Load a dataframe from file if a path is given, otherwise return the dataframe.

    Args:
        dataframe: A path to a file or a pandas DataFrame object.

    Returns:
        A pandas DataFrame object.
    """
    if isinstance(dataframe, pd.DataFrame):
        return dataframe

    # Get the file extension.
    if isinstance(dataframe, str) and dataframe.startswith("http"):
        ext = dataframe.split(".")[-1].lower()
    else:
        dataframe = Path(dataframe)
        ext = dataframe.suffix.lower()
        if not dataframe.exists():
            raise FileNotFoundError(f"File not found: {dataframe}")

    # Load the dataframe based on the file extension.
    match ext:
        case "csv":
            return pd.read_csv(dataframe)
        case "json":
            return pd.read_json(dataframe, lines=True)
        case "parquet":
            return pd.read_parquet(dataframe)
        case _:
            raise ValueError(f"Unsupported file format: {dataframe}")


class WithPrettyRepr:
    """Mixin offering stylized HTML and pretty rich console rendering of objects.

    For use in notebook displays of objects.
    """

    def __repr__(self) -> str:
        """Base Repr implementation.

        Puts dict fields on new lines for legibility.
        """
        field_repr = ",\n".join(f"  {k}={v!r}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{field_repr}\n)"

    def _repr_html_(self) -> str:
        """Represent the Repr string of an object as HTML.

        Assumes that the representation string of the object is given as
        a "python code" object. This is then rendered using Pygments and
        a module-standard CSS theming.
        """
        repr_string = self.__repr__()
        formatter = HtmlFormatter(style=DEFAULT_REPR_HTML_STYLE, cssclass="code")
        highlighted_html = highlight(repr_string, PythonLexer(), formatter)
        css = formatter.get_style_defs(".code")
        return REPR_HTML_TEMPLATE.format(css=css, highlighted_html=highlighted_html)

    def __rich_console__(self, console, options):
        yield Pretty(self)
