from pydantic import BaseModel, ConfigDict, Field

from gretel_client.data_designer.types import (
    AIDDColumnT,
    EvaluationReportT,
    ModelSuite,
    SeedDataset,
)
from gretel_client.workflows.configs.tasks import ColumnConstraint, PersonSamplerParams
from gretel_client.workflows.configs.workflows import ModelConfig


class AIDDConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    model_suite: ModelSuite
    model_configs: list[ModelConfig] | None = None
    seed_dataset: SeedDataset | None = None
    person_samplers: dict[str, PersonSamplerParams] | None = None
    columns: list[AIDDColumnT] = Field(min_length=1)
    constraints: list[ColumnConstraint] | None = None
    evaluation_report: EvaluationReportT | None = None
