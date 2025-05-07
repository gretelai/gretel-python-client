# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Union

from pydantic import Field

from gretel_client.workflows.configs.base import ConfigBase


class DistributionType(str, Enum):
    UNIFORM = "uniform"
    MANUAL = "manual"


class ManualDistributionParams(ConfigBase):
    values: Annotated[List[float], Field(min_length=1, title="Values")]
    weights: Annotated[Optional[List[float]], Field(title="Weights")] = None


class Step(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    task: Annotated[str, Field(title="Task")]
    inputs: Annotated[Optional[List[str]], Field(title="Inputs")] = None
    config: Annotated[Dict[str, Any], Field(title="Config")]


class UniformDistributionParams(ConfigBase):
    low: Annotated[float, Field(title="Low")]
    high: Annotated[float, Field(title="High")]


class ManualDistribution(ConfigBase):
    distribution_type: Optional[DistributionType] = "manual"
    params: ManualDistributionParams


class UniformDistribution(ConfigBase):
    distribution_type: Optional[DistributionType] = "uniform"
    params: UniformDistributionParams


class GenerationParameters(ConfigBase):
    temperature: Annotated[
        Optional[Union[float, UniformDistribution, ManualDistribution]],
        Field(title="Temperature"),
    ] = None
    top_p: Annotated[
        Optional[Union[float, UniformDistribution, ManualDistribution]],
        Field(title="Top P"),
    ] = None


class ModelConfig(ConfigBase):
    alias: Annotated[str, Field(title="Alias")]
    model_name: Annotated[str, Field(title="Model Name")]
    generation_parameters: GenerationParameters


class Globals(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = None
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    error_rate: Annotated[Optional[float], Field(title="Error Rate")] = 0.2


class Workflow(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    version: Annotated[Optional[str], Field(title="Version")] = "2"
    inputs: Annotated[Optional[Dict[str, Any]], Field(title="Inputs")] = None
    globals: Optional[Globals] = None
    steps: Annotated[Optional[List[Step]], Field(title="Steps")] = None
