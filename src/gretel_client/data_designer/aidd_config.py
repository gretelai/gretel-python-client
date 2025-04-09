import json

from pathlib import Path
from typing import Any

import yaml

from pydantic import Field

from gretel_client.data_designer.types import (
    AIDDColumnT,
    EvaluationReportT,
    ModelSuite,
    SeedDataset,
)
from gretel_client.workflows.configs.base import ConfigBase
from gretel_client.workflows.configs.tasks import ColumnConstraint, PersonSamplerParams
from gretel_client.workflows.configs.workflows import ModelConfig


class AIDDConfig(ConfigBase):
    model_suite: ModelSuite
    model_configs: list[ModelConfig] | None = None
    seed_dataset: SeedDataset | None = None
    person_samplers: dict[str, PersonSamplerParams] | None = None
    columns: list[AIDDColumnT] = Field(min_length=1)
    constraints: list[ColumnConstraint] | None = None
    evaluation_report: EvaluationReportT | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True)

    def to_yaml(
        self, path: str | Path | None = None, *, indent: int | None = 2, **kwargs
    ) -> str | None:
        yaml_str = yaml.dump(self.to_dict(), indent=indent, **kwargs)
        if path is None:
            return yaml_str
        with open(path, "w") as f:
            f.write(yaml_str)

    def to_json(
        self, path: str | Path | None = None, *, indent: int | None = 2, **kwargs
    ) -> str | None:
        json_str = json.dumps(self.to_dict(), indent=indent, **kwargs)
        if path is None:
            return json_str
        with open(path, "w") as f:
            f.write(json_str)
