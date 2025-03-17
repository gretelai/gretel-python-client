from typing import Optional

from pydantic import BaseModel, Field

from gretel_client.data_designer.types import (
    DataColumnT,
    EvaluatorT,
    ModelSuite,
    SeedDataset,
    ValidatorT,
)
from gretel_client.workflows.configs.tasks import ColumnConstraint
from gretel_client.workflows.configs.workflows import ModelConfig


class AIDDConfig(BaseModel):
    model_suite: ModelSuite
    model_configs: list[ModelConfig] | None = None
    seed_dataset: Optional[SeedDataset] = None
    columns: list[DataColumnT] = Field(min_length=1)
    constraints: list[ColumnConstraint] | None = None
    validators: list[ValidatorT] | None = None
    evaluators: list[EvaluatorT] | None = None
