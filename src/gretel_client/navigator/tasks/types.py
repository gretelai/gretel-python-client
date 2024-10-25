from enum import Enum
from typing import Optional, Union

from annotated_types import Len
from pydantic import BaseModel, Field
from typing_extensions import Annotated, Self

MAX_NUM_DATA_SEED_VALUES = 25
MAX_NUM_NESTED_DATA_SEEDS = 5

SeedValueT = Union[str, int, bool]


class LLMType(str, Enum):
    NL = "nl"
    CODE = "code"
    JUDGE = "judge"


class TextParserType(str, Enum):
    EXTRACT_CODE = "extract_code"
    JSON = "json"
    JSON_ARRAY = "json_array"
    PASS_THROUGH = "pass_through"


class CodeLang(str, Enum):
    PYTHON = "python"

    # SQL dialects match the SQLFluff naming conventions.
    ANSI = "ansi"
    TSQL = "tsql"
    BIGQUERY = "bigquery"
    MYSQL = "mysql"
    POSTGRES = "postgres"


SQL_DIALECTS = {
    CodeLang.ANSI,
    CodeLang.TSQL,
    CodeLang.BIGQUERY,
    CodeLang.MYSQL,
    CodeLang.POSTGRES,
}


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


class DataSeedColumns(BaseModel):
    seed_columns: list[DataSeedColumn]
