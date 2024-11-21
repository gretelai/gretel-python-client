from pathlib import Path
from typing import Optional, Union

import pandas as pd

from pydantic import BaseModel, Field

from gretel_client.navigator.client.interface import Client
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.constants import MAX_NUM_SEEDS
from gretel_client.navigator.tasks.types import (
    CategoricalDataSeeds,
    DEFAULT_MODEL_SUITE,
    ModelSuite,
    RecordsT,
    SystemPromptType,
)
from gretel_client.navigator.tasks.utils import process_sample_records

logger = get_logger(__name__, level="INFO")


class ExtractDataSeedsFromSampleRecordsConfig(BaseModel):
    sample_records: RecordsT
    max_num_seeds: int = Field(default=5, ge=1, le=MAX_NUM_SEEDS)
    num_assistants: int = Field(default=5, ge=1, le=8)
    dataset_context: str = ""
    system_prompt_type: SystemPromptType = SystemPromptType.COGNITION
    num_samples: int = 25


class ExtractDataSeedsFromSampleRecords(Task):

    def __init__(
        self,
        sample_records: Union[str, Path, pd.DataFrame, RecordsT],
        max_num_seeds: int = 5,
        num_assistants: int = 3,
        system_prompt_type: SystemPromptType = SystemPromptType.COGNITION,
        dataset_context: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        sample_records = process_sample_records(sample_records)
        super().__init__(
            config=ExtractDataSeedsFromSampleRecordsConfig(
                sample_records=sample_records,
                max_num_seeds=max_num_seeds,
                num_assistants=num_assistants,
                system_prompt_type=system_prompt_type,
                dataset_context=dataset_context or "",
                num_samples=len(sample_records),
            ),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @property
    def name(self) -> str:
        return "extract_data_seeds_from_sample_records"

    def run(self) -> CategoricalDataSeeds:
        return self._run()
