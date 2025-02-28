from typing import Any, Optional, Union

from pydantic import BaseModel, Field

from gretel_client.navigator.data_designer.data_column import GeneratedDataColumn
from gretel_client.navigator.tasks.types import (
    LLMJudgePromptTemplateType,
    ModelSuite,
    SeedCategory,
    ValidatorType,
)


class PostProcessor(BaseModel):
    settings: dict[str, Any]


class PostProcessValidator(PostProcessor):
    validator: ValidatorType


class PostProcessEvaluator(PostProcessor):
    evaluator: LLMJudgePromptTemplateType


class AIDDConfig(BaseModel):
    model_suite: ModelSuite
    categorical_seed_columns: list[SeedCategory] = Field(..., min_length=1)
    generated_data_columns: list[GeneratedDataColumn]
    post_processors: Optional[
        list[Union[PostProcessValidator, PostProcessEvaluator]]
    ] = None
