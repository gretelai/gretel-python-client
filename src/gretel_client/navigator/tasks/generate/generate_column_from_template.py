from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import (
    DEFAULT_MODEL_SUITE,
    LLMType,
    ModelSuite,
    TextParserType,
)

DEFAULT_RESPONSE_COLUMN_NAME = "response"


class GenerateColumnFromTemplateConfig(BaseModel):
    prompt_template: str
    response_column_name: str = DEFAULT_RESPONSE_COLUMN_NAME
    output_parser: TextParserType = TextParserType.PASS_THROUGH
    llm_type: LLMType = LLMType.NATURAL_LANGUAGE
    system_prompt: Optional[str] = None


class GenerateColumnFromTemplate(Task):
    """Generate a new column in a dataset based on a prompts template string.

    Within a Navigator Workflow, this task takes in a dataset as input and
    generates a new column based on the provided prompt template. Importantly,
    the prompt template must contain only keywords that are existing column names
    in the dataset. The generated column will be appended to the dataset.

    Args:
        prompt_template: Prompt template to be used for generating the column. The
            template should be a simple format string with keywords that are
            existing column names in the dataset.
        response_column_name: Name of the column to be generated.
        output_parser: The type of parser apply to the LLMs output. Must be a
            member of the TextParserType enum.
        llm_type: LLM type to use for generation. Must be a member of the LLMType enum.
        system_prompt: System prompt to use for generation. If not provided,
            a generic system prompt will be used.
        workflow_label: Label to append to the task name within a workflow. This can
            be helpful if you use the same task multiple times within a single workflow.
        client: Client object to use when running the task.
        model_suite: Suite of models to use. Must be a member of the ModelSuite enum.
    """

    def __init__(
        self,
        prompt_template: str,
        response_column_name: str = DEFAULT_RESPONSE_COLUMN_NAME,
        output_parser: TextParserType = TextParserType.PASS_THROUGH,
        llm_type: LLMType = LLMType.NATURAL_LANGUAGE,
        system_prompt: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        super().__init__(
            config=GenerateColumnFromTemplateConfig(
                prompt_template=prompt_template,
                response_column_name=response_column_name,
                output_parser=output_parser,
                llm_type=llm_type,
                system_prompt=system_prompt,
            ),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @property
    def name(self) -> str:
        return "generate_column_from_template"

    def run(self, dataset: Union[Dataset, list[dict]]) -> TaskOutput:
        return self._run(self._records_to_dataset_if_needed(dataset))
