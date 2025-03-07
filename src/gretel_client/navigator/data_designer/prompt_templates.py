from string import Formatter

from jinja2 import meta
from jinja2.sandbox import ImmutableSandboxedEnvironment

GENERATION_FSTRING_TEMPLATE_ERROR_MESSAGE = """\
Error with prompt template formatting! \
Make sure the template is not empty, uses named placeholders, \
and escapes any non-placeholder braces, e.g. {{ and }}. \
For more information, see \
https://docs.python.org/3/library/string.html#formatstrings
"""

GENERATION_JINJA_TEMPLATE_ERROR_MESSAGE = """\
Error with prompt template formatting! \
Make sure the template is not empty and follows \
Jinja2 template syntax. \
For more information, see \
https://jinja.palletsprojects.com/en/stable/templates/
"""


class GenerationTemplateError(Exception): ...


def is_jinja_template(user_template: str) -> bool:
    """Determine if a prompt template is a Jinja2 template from heuristics.

    This function is intended to help migration from format strings->Jinja.
    If we only support Jinja2, then this function is not needed.

    Args:
        user_template (str): A user-provided template string to test.

    Returns:
        True if the heuristic believes it is a Jinja2 template.
    """
    jinja_pattern_pairs = [("{{", "}}"), ("{%", "%}"), ("{#", "#}")]
    for open_pattern, close_pattern in jinja_pattern_pairs:
        if open_pattern in user_template and close_pattern in user_template:
            return True

    return False


def get_prompt_template_keywords(template: str) -> set[str]:
    """Extract all keywords from a string template."""
    if is_jinja_template(template):
        ast = ImmutableSandboxedEnvironment().parse(template)
        return set(meta.find_undeclared_variables(ast))

    return {
        k[1] for k in Formatter().parse(template) if len(k) > 1 and k[1] is not None
    }


def assert_valid_fstring_template(template: str) -> None:
    """Validate a prompt template string.

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
        raise GenerationTemplateError(GENERATION_FSTRING_TEMPLATE_ERROR_MESSAGE)

    try:
        # Formatter.parse returns an iterator that might raise ValueError upon iteration
        entries = list(Formatter().parse(template))
    except ValueError as exc:
        # This error occurs if there are issues like a single hanging brace.
        raise GenerationTemplateError(
            GENERATION_FSTRING_TEMPLATE_ERROR_MESSAGE
        ) from exc

    # If the entire template is literal text (i.e. no placeholders),
    # then it is considered valid.
    if len(entries) == 1 and entries[0][0] == template:
        return

    # Validate that every placeholder (if present) is a valid identifier.
    for _, field_name, _, _ in entries:
        if field_name is not None and not field_name.isidentifier():
            raise GenerationTemplateError(GENERATION_FSTRING_TEMPLATE_ERROR_MESSAGE)


def assert_valid_jinja_template(template: str) -> None:
    """Validate a Jinja prompt template.

    This is a lightweight validation meant to help parse syntax errors. It does
    not ensure that the Jinja template is *valid* in the sense of the generation
    task, which has further restrictions on the Jinja template.

    Basic validation:
      - Not empty
      - A valid Jinja format string.
    """
    if not template:
        raise GenerationTemplateError(GENERATION_JINJA_TEMPLATE_ERROR_MESSAGE)

    try:
        ## Verify that it parses
        ImmutableSandboxedEnvironment().parse(template)
    except Exception as exc:
        raise GenerationTemplateError(GENERATION_JINJA_TEMPLATE_ERROR_MESSAGE) from exc


def assert_valid_template(template: str) -> None:
    assert_fn = (
        assert_valid_jinja_template
        if is_jinja_template(template)
        else assert_valid_fstring_template
    )
    return assert_fn(template)


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
    "base": DATA_DESIGNER_BASE_SYSTEM_PROMPT,
    "natural_language": DATA_DESIGNER_NL_SYSTEM_PROMPT,
    "code": DATA_DESIGNER_CODE_SYSTEM_PROMPT,
    "judge": DATA_DESIGNER_JUDGE_SYSTEM_PROMPT,
}
