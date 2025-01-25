from typing import Optional

from pydantic import BaseModel, Field

from gretel_client.navigator.client.interface import Client
from gretel_client.navigator.data_designer.prompt_templates import (
    COLUMN_GENERATION_PROMPT,
    system_prompt_dict,
)
from gretel_client.navigator.tasks.generate.generate_column_from_template import (
    GenerateColumnFromTemplate,
)
from gretel_client.navigator.tasks.types import DataConfig, LLMType


class GeneratedDataColumn(BaseModel):
    """Generated data column to be used with DataDesigner.

    Generated data columns are fully generated by an LLM using the provided generation prompt.
    These data columns can be specified as a specific `output_type`, which triggers special handling
    for generation and validation.

    Args:
        name: The name of the data column.
        generation_prompt: The prompt that will be used to generate the data column. The prompt and can
            contain template keywords that reference seed columns or other existing data columns.
        columns_to_list_in_prompt: List of seed and/or data columns to list as context in the generation prompt.
        llm_type: LLM type for generation of the column. Must be one of ["natural_language", "code", "judge"].
        output_type: Specifies the nature of the column data. Must be one of ["text", "code", "structured"].
        output_type_params: Provides extra arguments used when interpreting the output type. See
            docstring notes for more information.
    """

    name: str
    generation_prompt: str
    columns_to_list_in_prompt: list[str] = Field(default_factory=list)
    llm_type: LLMType = LLMType.NATURAL_LANGUAGE
    data_config: DataConfig

    @property
    def prompt_template(self) -> str:
        """Generate the full prompt template for the column generation task.

        Returns:
            Full prompt template string.
        """
        return COLUMN_GENERATION_PROMPT.format(
            name=self.name,
            generation_prompt=self.generation_prompt,
            context=self.generate_context_column_string(),
        )

    def generate_context_column_string(self, exclude: Optional[set[str]] = None) -> str:
        """Generate the string for the relevant columns section of the prompt template.

        Args:
            exclude: Set of column names to exclude from the relevant columns section.

        Returns:
            Bullet-pointed string of relevant columns.
        """
        exclude = exclude or set()

        if len(set(self.columns_to_list_in_prompt) - exclude) == 0:
            return ""

        section_title = "\n### Relevant Context ###\n"
        return (
            section_title
            + "\n".join(
                [
                    f"    * {c.replace('_', ' ').capitalize()}: {{{c}}}"
                    for c in self.columns_to_list_in_prompt
                    if c not in exclude
                ]
            )
            + "\n"
        )

    def get_system_prompt(
        self, special_system_instructions: Optional[str] = None
    ) -> str:
        """Get the system prompt for the column generation task.

        Args:
            special_instructions: Special instructions to be added to the system prompt.

        Returns:
            System prompt string.
        """
        return system_prompt_dict[self.llm_type].format(
            special_instructions=(
                ""
                if special_system_instructions is None
                else f"\n{special_system_instructions}\n"
            )
        )

    def to_generation_task(
        self,
        special_system_instructions: Optional[str] = None,
        client: Optional[Client] = None,
    ) -> GenerateColumnFromTemplate:
        """Instantiate a GenerateColumnFromTemplate task from the DataColumn.

        Args:
            special_system_instructions: Special instructions to be added to the system prompt.
            client: The client to use for the task.

        Returns:
            GenerateColumnFromTemplate task instance.
        """
        return GenerateColumnFromTemplate(
            prompt_template=self.prompt_template,
            response_column_name=self.name,
            workflow_label=f"generating {self.name}",
            llm_type=self.llm_type,
            system_prompt=self.get_system_prompt(special_system_instructions),
            data_config=self.data_config,
            client=client,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\n"
            f"    name: {self.name}\n"
            f"    llm_type: {self.llm_type}\n"
            f"    column_type: {self.column_type}\n"
            f"    columns_to_list_in_prompt: {self.columns_to_list_in_prompt}\n"
            ")"
        )
