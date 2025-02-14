from string import Formatter

GENERATION_TEMPLATE_ERROR_MESSAGE = """\
Error with prompt template formatting! \
Make sure the template is not empty, uses named placeholders, \
and escapes any non-placeholder braces, e.g. {{ and }}. \
For more information, see \
https://docs.python.org/3/library/string.html#formatstrings.
"""


class GenerationTemplateError(Exception):
    def __init__(self, message=GENERATION_TEMPLATE_ERROR_MESSAGE):
        super().__init__(message)


def get_prompt_template_keywords(template: str) -> set[str]:
    """Extract all keywords from a string template."""
    return {
        k[1] for k in Formatter().parse(template) if len(k) > 1 and k[1] is not None
    }


def assert_valid_template(template: str) -> None:
    """Validate a template string.

    A valid template must:
      - Not be empty
      - Be a valid Python format string.
      - Use named placeholders (i.e. the placeholder names must be valid Python identifiers).

    Parameters:
        template (str): The template string to validate.

    Raises:
        GenerationTemplateError: If the template is invalid.
    """
    if not template:
        raise GenerationTemplateError()

    try:
        # Formatter.parse returns an iterator that might raise ValueError upon iteration
        entries = list(Formatter().parse(template))
    except ValueError as exc:
        # This error occurs if there are issues like a single hanging brace.
        raise GenerationTemplateError() from exc

    # If the entire template is literal text (i.e. no placeholders),
    # then it is considered valid.
    if len(entries) == 1 and entries[0][0] == template:
        return

    # Validate that every placeholder (if present) is a valid identifier.
    for _, field_name, _, _ in entries:
        if field_name is not None and not field_name.isidentifier():
            raise GenerationTemplateError()


DATA_DESIGNER_BASE_SYSTEM_PROMPT = """\
You always carefully consider all information provided to you, and you always follow all \
instructions. \
Unless requested, be as concise as possible in your responses; do not over explain. \
{llm_type_specific_instructions} \
{{special_instructions}}

YOU MUST GENERATE ALL OUTPUT IN ENGLISH ONLY.
"""

DATA_DESIGNER_NL_SYSTEM_PROMPT = DATA_DESIGNER_BASE_SYSTEM_PROMPT.format(
    llm_type_specific_instructions="""\
You are particularly adept at writing natural language, strictly adhering to \
all formatting constraints and instructions provided to you.
"""
)


DATA_DESIGNER_CODE_SYSTEM_PROMPT = DATA_DESIGNER_BASE_SYSTEM_PROMPT.format(
    llm_type_specific_instructions="""\
You are obsessed with writing excellent software that strictly adheres to all formatting \
constraints and instructions provided to you. You always use markdown code blocks to format your code, \
and you always respond with only the requested code, without any preamble or additional text. \
Importantly, you ALWAYS write self-contained code.
"""
)

COLUMN_GENERATION_PROMPT = """\
{context}{generation_prompt}\
"""

DATA_DESIGNER_JUDGE_SYSTEM_PROMPT = """\
You are a fair and experienced judge. \
You are particularly adept at writing natural language. \
You must judge in English only."""

system_prompt_dict = {
    "natural_language": DATA_DESIGNER_NL_SYSTEM_PROMPT,
    "code": DATA_DESIGNER_CODE_SYSTEM_PROMPT,
    "judge": DATA_DESIGNER_JUDGE_SYSTEM_PROMPT,
}
