from typing import Optional, Union

import pandas as pd

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import LLMType, TextParserType

DEFAULT_RESPONSE_COLUMN_NAME = "response"


class GenerateColumnFromTemplateConfig(BaseModel):
    prompt_template: str
    response_column_name: str = DEFAULT_RESPONSE_COLUMN_NAME
    output_parser: TextParserType = TextParserType.PASS_THROUGH
    llm_type: LLMType = LLMType.NL
    system_prompt: Optional[str] = None


class GenerateColumnFromTemplate(Task):

    def __init__(
        self,
        prompt_template: str,
        response_column_name: str = DEFAULT_RESPONSE_COLUMN_NAME,
        output_parser: TextParserType = TextParserType.PASS_THROUGH,
        llm_type: LLMType = LLMType.NL,
        system_prompt: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
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
        )

    @property
    def name(self) -> str:
        return "generate_column_from_template"

    def run(self, template_kwargs: Union[Dataset, list[dict]]) -> TaskOutput:
        if isinstance(template_kwargs, list):
            template_kwargs = pd.DataFrame(template_kwargs)
        return self._run(template_kwargs)
