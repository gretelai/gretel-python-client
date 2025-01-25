from string import Formatter


def get_prompt_template_keywords(template: str) -> set[str]:
    """Extract all keywords from a string template."""
    return {
        k[1] for k in Formatter().parse(template) if len(k) > 1 and k[1] is not None
    }


DATA_DESIGNER_BASE_SYSTEM_PROMPT = """\
You are an expert data practioner, skilled in the creation of diverse, high-quality datasets, \
leveraging deep expertise across domains in both the academic and industry sectors. \
You always carefully consider all information provided to you, and you always follow all \
instructions. \
{llm_type_specific_instructions} \
{{special_instructions}}
You always provide your response after the '### Response ###' section header.

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
Your task is to generate the `{name}` column in a dataset based on the Generation Prompt below. \
It is VERY IMPORTANT that you follow ALL instructions carefully.

### Generation Prompt ###
{generation_prompt}
{context}\

### General Instructions ###
    * Generate the `{name}` column as described by the Generation Prompt.
    * Remember to generate all output in English.
    * Base your response on the above information.

### Response ###
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
