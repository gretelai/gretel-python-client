import inspect
import json
import re

from datetime import date
from typing import Any, Type

import requests

from pydantic import BaseModel

from gretel_client.data_designer.constants import TASK_TYPE_EMOJI_MAP
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
