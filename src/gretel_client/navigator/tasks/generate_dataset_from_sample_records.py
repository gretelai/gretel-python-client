from pathlib import Path
from typing import Optional, Union

import pandas as pd

from pydantic import BaseModel, Field

from gretel_client.navigator.client.interface import Client
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.constants import MAX_SAMPLE_SIZE
from gretel_client.navigator.tasks.types import (
    CategoricalDataSeeds,
    DEFAULT_MODEL_SUITE,
    ModelSuite,
    RecordsT,
    SystemPromptType,
)
from gretel_client.navigator.tasks.utils import process_sample_records

logger = get_logger(__name__, level="INFO")


class GenerateDatasetFromSampleRecordsConfig(BaseModel):
    sample_records: RecordsT
    target_num_records: int = Field(500, ge=MAX_SAMPLE_SIZE, le=10_000)
    system_prompt_type: SystemPromptType = SystemPromptType.COGNITION
    num_records_per_seed: int = Field(5, ge=1, le=10)
    num_examples_per_prompt: int = Field(5, ge=1, le=MAX_SAMPLE_SIZE)
    dataset_context: str = ""


class GenerateDatasetFromSampleRecords(Task):

    def __init__(
        self,
        sample_records: Union[str, Path, pd.DataFrame, RecordsT],
        target_num_records: int = 500,
        system_prompt_type: SystemPromptType = SystemPromptType.COGNITION,
        num_records_per_seed: int = 5,
        num_examples_per_prompt: int = 5,
        dataset_context: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        sample_records = process_sample_records(sample_records)
        super().__init__(
            config=GenerateDatasetFromSampleRecordsConfig(
                sample_records=sample_records,
                target_num_records=target_num_records,
                system_prompt_type=system_prompt_type,
                num_records_per_seed=num_records_per_seed,
                num_examples_per_prompt=num_examples_per_prompt,
                dataset_context=dataset_context or "",
            ),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @property
    def name(self) -> str:
        return "generate_dataset_from_sample_records"

    def run(
        self, categorical_data_seeds: Union[dict, CategoricalDataSeeds]
    ) -> CategoricalDataSeeds:
        if categorical_data_seeds and isinstance(categorical_data_seeds, dict):
            categorical_data_seeds = CategoricalDataSeeds(**categorical_data_seeds)
        return self._run(
            {
                "type": "categorical_data_seeds",
                "obj": categorical_data_seeds.model_dump(),
            }
        )
