from enum import Enum

from gretel_client.workflows.configs.tasks import CodeLang

SQL_DIALECTS = {
    CodeLang.SQLITE,
    CodeLang.TSQL,
    CodeLang.BIGQUERY,
    CodeLang.MYSQL,
    CodeLang.POSTGRES,
    CodeLang.ANSI,
}

VALIDATE_PYTHON_COLUMN_SUFFIXES = [
    "_pylint_score",
    "_pylint_severity",
    "_pylint_messages",
]

VALIDATE_SQL_COLUMN_SUFFIXES = [
    "_validator_messages",
]

TASK_TYPE_EMOJI_MAP = {
    "generate": "🦜",
    "evaluate": "🧐",
    "validate": "🔍",
    "judge": "⚖️",
    "sample": "🎲",
    "seed": "🌱",
    "load": "📥",
    "extract": "💭",
}


DEFAULT_REPR_HTML_STYLE = "nord"

REPR_HTML_TEMPLATE = (
    '<meta charset="UTF-8">'
    "<style>{css}</style>"
    "<div class='code' "
    "style='padding: 5px;"
    "border: 1px solid grey;"
    "border-radius: 5px;"
    "display:inline-block;'>"
    "{highlighted_html}"
    "</div>"
)


MODEL_DUMP_KWARGS = dict(
    exclude_unset=False,
    exclude={"model_configs", "model_suite", "error_rate"},
    mode="json",
)


class NordColor(Enum):
    NORD0 = "#2E3440"  # Darkest gray (background)
    NORD1 = "#3B4252"  # Dark gray
    NORD2 = "#434C5E"  # Medium dark gray
    NORD3 = "#4C566A"  # Lighter dark gray
    NORD4 = "#D8DEE9"  # Light gray (default text)
    NORD5 = "#E5E9F0"  # Very light gray
    NORD6 = "#ECEFF4"  # Almost white
    NORD7 = "#8FBCBB"  # Teal
    NORD8 = "#88C0D0"  # Light cyan
    NORD9 = "#81A1C1"  # Soft blue
    NORD10 = "#5E81AC"  # Darker blue
    NORD11 = "#BF616A"  # Red
    NORD12 = "#D08770"  # Orange
    NORD13 = "#EBCB8B"  # Yellow
    NORD14 = "#A3BE8C"  # Green
    NORD15 = "#B48EAD"  # Purple
