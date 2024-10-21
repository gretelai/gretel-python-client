from pathlib import Path
from typing import Optional, Union

from annotated_types import Len
from pydantic import BaseModel, Field
from typing_extensions import Annotated, Self

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.tasks.base import Task, TaskResults

SeedValueT = Union[str, int, bool]


MAX_NUM_DATA_SEED_VALUES = 25
MAX_NUM_NESTED_DATA_SEEDS = 5


class SeedColumn(BaseModel):
    name: str
    description: Optional[str] = None

    @classmethod
    def from_dicts(cls, seeds: list[dict]) -> list[Self]:
        return [cls(**seed) for seed in seeds]


class NestedDataSeedColumn(SeedColumn):
    num_values_to_generate: int = Field(default=1, gt=0, le=MAX_NUM_DATA_SEED_VALUES)
    generated_values: dict[str, list[SeedValueT]] = {}


class DataSeedColumn(SeedColumn):
    values: list[SeedValueT] = Field(default=[])
    weights: list[float] = Field(default=[])
    num_values_to_generate: Optional[int] = Field(
        default=None, gt=0, le=MAX_NUM_DATA_SEED_VALUES
    )
    nested_data_seeds: Annotated[
        list[NestedDataSeedColumn], Len(max_length=MAX_NUM_NESTED_DATA_SEEDS)
    ] = []
    quality_rank: Optional[int] = None
    generated_values: list[SeedValueT] = []


class GenerateSeedValuesConfig(BaseModel):
    seed_columns: list[DataSeedColumn]
    dataset_context: str = ""


class GenerateSeedValues(Task):

    def __init__(
        self,
        seed_columns: Union[str, Path, list[dict], list[DataSeedColumn]],
        dataset_context: Optional[str] = None,
        workflow_label: Optional[str] = None,
    ):
        super().__init__(
            config=GenerateSeedValuesConfig(
                seed_columns=self._check_and_get_seed_columns(seed_columns),
                dataset_context=dataset_context or "",
            ),
            workflow_label=workflow_label,
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

    def run(self) -> TaskResults:
        return self._run()
