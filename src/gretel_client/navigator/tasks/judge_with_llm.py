from dataclasses import dataclass
from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import (
    DEFAULT_MODEL_SUITE,
    LLMJudgePromptTemplateType,
    ModelSuite,
    RecordsT,
)


class JudgeWithLLMConfig(BaseModel):
    judge_template_type: LLMJudgePromptTemplateType
    instruction_column_name: str
    response_column_name: str
    context_column_name: Optional[str] = None
    num_samples_to_judge: Optional[int] = 100


@dataclass(frozen=True)
class JudgeRubric:
    # When adding new rubrics, ensure all rubric names are capitalized to match jarvis
    text_to_python = {
        "Relevance": "Adherence to INSTRUCTIONS",
        "Readability": "Readability and Maintainability (Is the Python code easy to understand and maintain?)",
        "Efficiency": "Efficiency and Performance (Is the code optimized for performance?)",
        "Pythonic": "Pythonic Code and Best Practices (Does the code follow Python conventions and best practices?)",
    }

    text_to_sql = {
        "Relevance": "Adherence to INSTRUCTIONS and CONTEXT",
        "Readability": "Readability and Maintainability (Is the SQL code easy to understand and maintain?)",
        "Scalability": "Scalability (Does the solution scale well with larger datasets or more complex queries?)",
        "Standards": "Compliance with Standards (Does the SQL query follow SQL standards and best practices?)",
    }

    @classmethod
    def get_rubric(cls, eval_type: LLMJudgePromptTemplateType):
        if eval_type == LLMJudgePromptTemplateType.TEXT_TO_PYTHON:
            return cls.text_to_python
        elif eval_type == LLMJudgePromptTemplateType.TEXT_TO_SQL:
            return cls.text_to_sql
        else:
            raise ValueError(f"Unsupported judge template type: {eval_type}")


class JudgeWithLLM(Task):

    def __init__(
        self,
        judge_template_type: LLMJudgePromptTemplateType,
        instruction_column_name: str,
        response_column_name: str,
        context_column_name: Optional[str] = None,
        num_samples_to_judge: Optional[int] = 100,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        super().__init__(
            config=JudgeWithLLMConfig(
                judge_template_type=judge_template_type,
                instruction_column_name=instruction_column_name,
                response_column_name=response_column_name,
                context_column_name=context_column_name,
                num_samples_to_judge=num_samples_to_judge,
            ),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @property
    def name(self) -> str:
        return "judge_with_llm"

    def run(self, dataset: Union[Dataset, RecordsT]) -> TaskOutput:
        return self._run(self._records_to_dataset_if_needed(dataset))
