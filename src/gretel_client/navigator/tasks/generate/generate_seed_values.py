from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.types import DataSeedColumn


class GenerateSeedValuesConfig(BaseModel):
    seed_columns: list[DataSeedColumn]
    dataset_context: str = ""


class GenerateSeedValues(Task):

    def __init__(
        self,
        seed_columns: Union[str, Path, list[dict], list[DataSeedColumn]],
        dataset_context: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=GenerateSeedValuesConfig(
                seed_columns=self._check_and_get_seed_columns(seed_columns),
                dataset_context=dataset_context or "",
            ),
            workflow_label=workflow_label,
            client=client,
        )

    @staticmethod
    def _check_and_get_seed_columns(
        columns: Union[str, Path, list[dict], list[DataSeedColumn]]
    ) -> list[DataSeedColumn]:
        if isinstance(columns, (str, Path)):
            columns = smart_load_yaml(columns).get("seed_columns")

        if not isinstance(columns, list):
            raise ValueError(
                "`columns` must be a list of dicts or DataSeedColumn objects"
            )

        # Convert dicts to DataSeedColumn objects to ensure they are valid.
        if all(isinstance(seed, dict) for seed in columns):
            columns = DataSeedColumn.from_dicts(columns)

        if not all(isinstance(seed, DataSeedColumn) for seed in columns):
            raise ValueError(
                "`columns` must be a list of dicts or DataSeedColumn objects"
            )

        return columns

    @property
    def name(self) -> str:
        return "generate_seed_values"

    def run(self) -> TaskOutput:
        return self._run()
