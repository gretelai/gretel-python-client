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
