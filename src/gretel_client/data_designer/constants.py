from enum import Enum

from rich.theme import Theme

from gretel_client.workflows.configs.tasks import CodeLang

SQL_DIALECTS = {
    CodeLang.SQL_SQLITE,
    CodeLang.SQL_TSQL,
    CodeLang.SQL_BIGQUERY,
    CodeLang.SQL_MYSQL,
    CodeLang.SQL_POSTGRES,
    CodeLang.SQL_ANSI,
}

VALIDATE_PYTHON_COLUMN_SUFFIXES = [
    "_python_linter_score",
    "_python_linter_severity",
    "_python_linter_messages",
]

VALIDATE_SQL_COLUMN_SUFFIXES = [
    "_validator_messages",
]

TASK_TYPE_EMOJI_MAP = {
    "generating": "🦜",
    "evaluating": "🧐",
    "validating": "🔍",
    "using-llm-to-judge": "⚖️",
    "using-samplers": "🎲",
    "rendering": "💬",
    "seeding": "🌱",
    "seed": "🌱",
    "load": "📥",
    "dropping": "🙈",
    "extract": "💭",
    "concatenating": "🔗",
}

DEFAULT_REPR_HTML_STYLE = "nord"

REPR_LIST_LENGTH_USE_JSON = 4

REPR_HTML_FIXED_WIDTH = 1000
REPR_HTML_TEMPLATE = """
<meta charset="UTF-8">
<style>
{{css}}

.code {{{{
  padding: 4px;
  border: 1px solid grey;
  border-radius: 4px;
  max-width: {fixed_width}px;
  width: 100%;
  display: inline-block;
  box-sizing: border-box;
  text-align: left;
  vertical-align: top;
  line-height: normal;
  overflow-x: auto;
}}}}

.code pre {{{{
  white-space: pre-wrap;       /* CSS 3 */
  white-space: -moz-pre-wrap;  /* Mozilla, since 1999 */
  white-space: -pre-wrap;      /* Opera 4-6 */
  white-space: -o-pre-wrap;    /* Opera 7 */
  word-wrap: break-word;
  overflow-wrap: break-word;
  margin: 0;     
}}}}
</style>
{{highlighted_html}}
""".format(fixed_width=REPR_HTML_FIXED_WIDTH)


MODEL_DUMP_KWARGS = dict(exclude={"model_configs", "model_suite", "error_rate"})

NUM_PREVIEW_RECORDS = 10


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


RICH_CONSOLE_THEME = Theme(
    {
        "repr.number": NordColor.NORD15.value,  # Purple for numbers
        "repr.string": NordColor.NORD14.value,  # Green for strings
        "repr.bool_true": NordColor.NORD9.value,  # Blue for True
        "repr.bool_false": NordColor.NORD9.value,  # Blue for False
        "repr.none": NordColor.NORD11.value,  # Red for None
        "repr.brace": NordColor.NORD7.value,  # Teal for brackets/braces
        "repr.comma": NordColor.NORD7.value,  # Teal for commas
        "repr.ellipsis": NordColor.NORD7.value,  # Teal for ellipsis
        "repr.attrib_name": NordColor.NORD3.value,  # Light gray for dict keys
        "repr.attrib_equal": NordColor.NORD7.value,  # Teal for equals signs
        "repr.call": NordColor.NORD10.value,  # Darker blue for function calls
        "repr.function_name": NordColor.NORD10.value,  # Darker blue for function names
        "repr.class_name": NordColor.NORD12.value,  # Orange for class names
        "repr.module_name": NordColor.NORD8.value,  # Light cyan for module names
        "repr.error": NordColor.NORD11.value,  # Red for errors
        "repr.warning": NordColor.NORD13.value,  # Yellow for warnings
    }
)


DEFAULT_HIST_NAME_COLOR = "medium_purple1"

DEFAULT_HIST_VALUE_COLOR = "pale_green3"
