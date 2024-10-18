from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.navigator.tasks.base import Task, TaskResults
from gretel_client.navigator.tasks.io import Dataset

DEFAULT_RESPONSE_COLUMN_NAME = "response"


class LLMType(str, Enum):
    NL = "nl"
    CODE = "code"
    JUDGE = "judge"


class TextParserType(str, Enum):
    EXTRACT_CODE = "extract_code"
    JSON = "json"
    JSON_ARRAY = "json_array"
    PASS_THROUGH = "pass_through"


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
        )

    @property
    def name(self) -> str:
        return "generate_column_from_template"

    def run(self, template_kwargs: Union[Dataset, list[dict]]) -> TaskResults:
        return self._run(dataset=template_kwargs)
