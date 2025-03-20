from typing import Annotated, Dict, Optional, Type, Union

from annotated_types import Len
from pydantic import BaseModel, Field
from typing_extensions import TypeAlias

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import (
    DEFAULT_MODEL_SUITE,
    LLMType,
    ModelSuite,
    RecordsT,
)

Scoring: TypeAlias = Union[Dict[int, str], Dict[str, str]]


class Rubric(BaseModel):
    """Specifies a scoring rubric."""

    scoring: Scoring = Field(
        ...,
        description="Dictionary specifying score: description pairs for rubric scoring.",
    )
    name: str = Field(..., description="A clear, pythonic class name for this rubric.")
    description: str = Field(
        default="",
        description="An informative and detailed assessment guide for using this rubric.",
    )


class JudgeWithLLMConfig(BaseModel):
    prompt: str = Field(
        ...,
        description="Template for generating prompts. Use {{column_name}} placeholders to reference dataset columns.",
    )
    num_samples_to_judge: Optional[int] = Field(
        default=100,
        description="Number of samples to judge. Default is 100.",
    )
    rubrics: Annotated[list[Rubric], Len(min_length=1)] = Field(
        ...,
        description="List of rubric configurations to use for evaluation. At least one must be provided.",
    )


class JudgeWithLLM(Task):

    def __init__(
        self,
        prompt: str,
        rubrics: list[Type[Rubric]],
        num_samples_to_judge: Optional[int] = 100,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        model_alias: LLMType = LLMType.JUDGE,
    ):
        # pass a list of dictionaries to the jarvis task ↓
        rubrics = [
            config.model_dump() if isinstance(config, Rubric) else config
            for config in rubrics
        ]

        super().__init__(
            config=JudgeWithLLMConfig(
                prompt=prompt,
                num_samples_to_judge=num_samples_to_judge,
                rubrics=rubrics,
                model_alias=model_alias,
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
