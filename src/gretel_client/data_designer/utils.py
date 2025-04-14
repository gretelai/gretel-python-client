import collections
import inspect
import json
import re

from contextlib import contextmanager
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Generic, Self, Type, TypeVar

import pandas as pd
import requests

from jinja2 import meta, TemplateSyntaxError
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import BaseModel
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer

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


def _split_camel_case(s: str, sep: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", sep, s).lower()


def camel_to_kebab(s: str) -> str:
    return _split_camel_case(s, "-")


def camel_to_snake(s: str) -> str:
    return _split_camel_case(s, "_")


def make_date_obj_serializable(obj: dict) -> dict:

    class DateTimeEncoder(json.JSONEncoder):

        def default(self, obj: Any) -> Any:
            if isinstance(obj, date):
                return obj.isoformat()
            return super().default(obj)

    return json.loads(json.dumps(obj, cls=DateTimeEncoder))


def code_lang_to_syntax_lexer(code_lang: tasks.CodeLang | str) -> str:
    """Convert the code language to a syntax lexer for Pygments.

    Reference: https://pygments.org/docs/lexers/
    """
    match code_lang:
        case tasks.CodeLang.GO:
            return "golang"
        case tasks.CodeLang.JAVASCRIPT:
            return "javascript"
        case tasks.CodeLang.JAVA:
            return "java"
        case tasks.CodeLang.KOTLIN:
            return "kotlin"
        case tasks.CodeLang.PYTHON:
            return "python"
        case tasks.CodeLang.RUBY:
            return "ruby"
        case tasks.CodeLang.RUST:
            return "rust"
        case tasks.CodeLang.SCALA:
            return "scala"
        case tasks.CodeLang.SWIFT:
            return "swift"
        case tasks.CodeLang.SQL_SQLITE:
            return "sql"
        case tasks.CodeLang.SQL_ANSI:
            return "sql"
        case tasks.CodeLang.SQL_TSQL:
            return "tsql"
        case tasks.CodeLang.SQL_BIGQUERY:
            return "sql"
        case tasks.CodeLang.SQL_MYSQL:
            return "mysql"
        case tasks.CodeLang.SQL_POSTGRES:
            return "postgres"
        case _:
            return code_lang


class UserJinjaTemplateSyntaxError(Exception): ...


@contextmanager
def template_error_handler():
    try:
        yield
    except TemplateSyntaxError as exception:
        exception_string = (
            f"Encountered a syntax error in the provided Jinja2 template:\n{str(exception)}\n"
            "For more information on writing Jinja2 templates, refer to https://jinja.palletsprojects.com/en/stable/templates"
        )
        raise UserJinjaTemplateSyntaxError(exception_string)
    except Exception:
        raise


def assert_valid_jinja2_template(template: str) -> None:
    """Raises an error if the template cannot be parsed."""
    with template_error_handler():
        meta.find_undeclared_variables(ImmutableSandboxedEnvironment().parse(template))


def get_prompt_template_keywords(template: str) -> set[str]:
    """Extract all keywords from a valid string template."""
    with template_error_handler():
        ast = ImmutableSandboxedEnvironment().parse(template)
        keywords = set(meta.find_undeclared_variables(ast))

    return keywords


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

    # TODO: Remove when autogenerated params are easy to associate with their source.
    for source in tasks.SamplerType:
        source_name = source.value.replace("_", "")
        # Iterate in reverse order so the shortest match is first.
        # This is necessary for params that start with the same name.
        # For example, "bernoulli" and "bernoulli_mixture".
        params_cls_dict[source.value] = [
            params_cls
            for params_cls in reversed(params_cls_list)
            # Match param type string with parameter class.
            # For example, "gaussian" -> "GaussianSamplerParams"
            if source_name == params_cls.__name__.lower()[: len(source_name)]
            # Take the first match.
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

    _repr_float_precision: int = 3

    @staticmethod
    def _get_display_value(v: Any, precision: int) -> Any:
        """Intercept values for custom redisplay.

        Args:
            v (Any): The value to display.
            precision (int): number of decimal digits to display
                for floating point values.

        Returns:
            A value to use for repr to display.
        """
        if isinstance(v, float):
            return round(v, precision)

        elif isinstance(v, Enum):
            return v.value

        elif isinstance(v, BaseModel):
            return WithPrettyRepr._get_display_value(
                v.model_dump(mode="json"), precision
            )

        elif isinstance(v, list):
            return [WithPrettyRepr._get_display_value(x, precision) for x in v]

        elif isinstance(v, set):
            return {WithPrettyRepr._get_display_value(x, precision) for x in v}

        elif isinstance(v, dict):
            return {
                k: WithPrettyRepr._get_display_value(x, precision) for k, x in v.items()
            }

        return v

    def _kv_to_string(self, k: str, v: Any) -> str:
        v_display = self._get_display_value(v, self._repr_float_precision)
        return f"    {k}={v_display!r}"

    def __repr__(self) -> str:
        """Base Repr implementation.

        Puts dict fields on new lines for legibility.
        """
        dict_repr = (
            self.model_dump(mode="json", exclude_unset=True)
            if isinstance(self, BaseModel)
            else self.__dict__
        )
        field_repr = ",\n".join(
            self._kv_to_string(k, v)
            for k, v in dict_repr.items()
            if not k.startswith("_")
        )
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
        yield self.__repr__()


KT = TypeVar("KT")
VT = TypeVar("VT")


class CallbackOnMutateDict(collections.UserDict, Generic[KT, VT]):
    """
    A dictionary-like class that calls a callback function
    whenever its contents are mutated.
    """

    _callback: Callable[[], None]

    def __init__(
        self,
        callback: Callable[[], None],
    ):
        if not callable(callback):
            raise TypeError("callback must be callable")

        super().__init__()
        self._callback = callback

    def __setitem__(self, key: KT, value: VT):
        """Called for: d[key] = value"""
        super().__setitem__(key, value)
        self._callback()

    def __delitem__(self, key: KT):
        """Called for: del d[key]"""
        super().__delitem__(key)
        self._callback()

    def __ior__(self, other) -> Self:
        super().__ior__(other)
        self._callback()
        return self
