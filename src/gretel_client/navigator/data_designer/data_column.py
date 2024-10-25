from typing import Literal, Optional

from pydantic import BaseModel, Field

from gretel_client.navigator.client.interface import Client
from gretel_client.navigator.data_designer.prompt_templates import (
    COLUMN_GENERATION_PROMPT,
    get_prompt_template_keywords,
    system_prompt_dict,
)
from gretel_client.navigator.tasks.generate.generate_column_from_template import (
    GenerateColumnFromTemplate,
    TextParserType,
)

parser_instructions_map = {
    TextParserType.PASS_THROUGH: (
        "Respond only with the requested text, "
        "without any additional comments or instructions."
    ),
    TextParserType.JSON_ARRAY: "Respond only with a list as a valid JSON array.",
    TextParserType.EXTRACT_CODE: (
        "Respond only with the requested code, "
        "without any preamble or additional text."
    ),
}

output_parser_type_map = {
    "str": TextParserType.PASS_THROUGH,
    "string": TextParserType.PASS_THROUGH,
    "text": TextParserType.PASS_THROUGH,
    "json": TextParserType.JSON,
    "dict": TextParserType.JSON,
    "list": TextParserType.JSON_ARRAY,
    "json_array": TextParserType.JSON_ARRAY,
    "code": TextParserType.EXTRACT_CODE,
}


class DataColumn(BaseModel):
    name: str
    description: str
    output_type: Literal["text", "dict", "list", "code"] = "text"
    relevant_columns: list[str] = Field(default_factory=list)
    specific_instructions: str = ""
    llm_type: Literal["nl", "code"] = "nl"

    def get_context_list_string(self, exclude: Optional[set[str]] = None) -> str:
        exclude = exclude or set()

        if len(set(self.relevant_columns) - exclude) == 0:
            return ""

        section_title = "\n### Other Relevant Data ###\n"
        return (
            section_title
            + "\n".join(
                [
                    f"    * {c.replace('_', ' ').capitalize()}: {{{c}}}"
                    for c in self.relevant_columns
                    if c not in exclude
                ]
            )
            + "\n"
        )

    def to_generation_task(
        self,
        special_system_instructions: Optional[str] = None,
        client: Optional[Client] = None,
    ) -> GenerateColumnFromTemplate:

        extra = ""
        specific = ""
        if len(self.specific_instructions) > 0:
            specific = (
                f"\n### Specific Instructions ###\n{self.specific_instructions}\n"
            )
            extra = "\n    * Pay particularly close attention to the above Specific Instructions."

        output_parser = output_parser_type_map[self.output_type]

        # Exclude relevant_columns that are present in the specific instructions.
        exclude = set()
        for key in get_prompt_template_keywords(specific):
            if {key}.issubset(self.relevant_columns):
                exclude |= {key}

        return GenerateColumnFromTemplate(
            prompt_template=COLUMN_GENERATION_PROMPT.format(
                name=self.name,
                description=self.description,
                specific_instructions=specific,
                context=self.get_context_list_string(exclude),
                parser_instructions=parser_instructions_map[output_parser],
                extra_instructions=extra,
            ),
            output_parser=output_parser,
            response_column_name=self.name,
            workflow_label=f"generating {self.name}",
            llm_type=self.llm_type,
            system_prompt=system_prompt_dict[self.llm_type].format(
                special_instructions=(
                    ""
                    if special_system_instructions is None
                    else f"\n{special_system_instructions}\n"
                )
            ),
            client=client,
        )
