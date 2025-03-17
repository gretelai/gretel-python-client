import json
import re

from datetime import date
from typing import Any

import requests

from gretel_client.data_designer.constants import TASK_TYPE_EMOJI_MAP
from gretel_client.data_designer.types import CodeLang


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


def code_lang_to_syntax_lexer(code_lang: CodeLang) -> str:
    """Convert the code language to a syntax lexer for Pygments.

    Reference: https://pygments.org/docs/lexers/
    """
    match code_lang:
        case CodeLang.PYTHON:
            return "python"
        case CodeLang.SQLITE:
            return "sql"
        case CodeLang.ANSI:
            return "sql"
        case CodeLang.TSQL:
            return "tsql"
        case CodeLang.BIGQUERY:
            return "sql"
        case CodeLang.MYSQL:
            return "mysql"
        case CodeLang.POSTGRES:
            return "postgres"
        case _:
            raise ValueError(f"Unsupported code language: {code_lang}")
