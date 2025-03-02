import json

from enum import Enum
from typing import Any, Optional, Type, Union

from annotated_types import Len
from pydantic import BaseModel, Field, field_serializer, model_validator
from pydantic.functional_serializers import SerializeAsAny
from rich.console import Console, Group
from rich.pretty import Pretty
from rich.text import Text
from typing_extensions import Annotated, Self

MAX_NUM_DATA_SEED_VALUES = 25
MAX_NUM_NESTED_DATA_SEED_SUBCATEGORIES = 5

RecordsT = list[dict[str, Any]]
SeedValueT = Union[str, int, bool]


class ModelSuite(str, Enum):
    APACHE_2_0 = "apache-2.0"
    LLAMA_3_x = "llama-3.x"


DEFAULT_MODEL_SUITE = ModelSuite.APACHE_2_0


def check_model_suite(model_suite: Union[ModelSuite, str]) -> str:
    # Temporarily disable this check for now.
    # is_gretel_dev = get_session_config().stage == "dev"

    # if not is_gretel_dev:
    # Make sure that the model_suite is a valid ModelSuite enum.
    # Why? Faster feedback for users who are using the wrong model suite.
    # return ModelSuite(model_suite).value

    # Allow for more flexibility in dev mode.
    if isinstance(model_suite, ModelSuite):
        return model_suite.value
    return model_suite


class OutputColumnType(str, Enum):
    TEXT = "text"
    CODE = "code"
    STRUCTURED = "structured"


class ColumnType(BaseModel): ...


class TextColumnType(ColumnType): ...


class CodeColumnType(ColumnType):
    syntax: str


class StructuredColumnType(ColumnType):
    model: Annotated[Optional[Type[BaseModel]], Field(exclude=True)] = None
    json_schema: dict

    @model_validator(mode="before")
    @classmethod
    def populate_json_schema(cls, data: Any) -> Any:
        if "json_schema" not in data:
            model = data.get("model")
            if model is not None:
                data["json_schema"] = model.model_json_schema()
        elif isinstance(data["json_schema"], str):
            data["json_schema"] = json.loads(data["json_schema"])
        return data


_COLUMN_TYPES = {
    OutputColumnType.CODE: CodeColumnType,
    OutputColumnType.TEXT: TextColumnType,
    OutputColumnType.STRUCTURED: StructuredColumnType,
}


class DataConfig(BaseModel):
    type: OutputColumnType
    params: SerializeAsAny[ColumnType]

    @model_validator(mode="before")
    @classmethod
    def populate_params(cls, data: dict) -> dict:
        t, p = data.get("type"), data.get("params", {})
        if not t:
            raise KeyError("No type specified.")

        if t not in _COLUMN_TYPES:
            raise ValueError(f"Unknown column type {t}")

        data["params"] = _COLUMN_TYPES[t].model_validate(p)
        return data

    @field_serializer("type")
    def serialize_type(self, type: OutputColumnType, _) -> str:
        return str(type.value)


class SystemPromptType(str, Enum):
    REFLECTION = "reflection"
    COGNITION = "cognition"


class LLMType(str, Enum):
    NATURAL_LANGUAGE = "natural_language"
    CODE = "code"
    JUDGE = "judge"


class TextParserType(str, Enum):
    EXTRACT_CODE = "extract_code"
    JSON = "json"
    JSON_ARRAY = "json_array"
    PASS_THROUGH = "pass_through"


class EvaluationType(str, Enum):
    GENERAL = "general"


class LLMJudgePromptTemplateType(str, Enum):
    TEXT_TO_PYTHON = "text_to_python"
    TEXT_TO_SQL = "text_to_sql"


class CodeLang(str, Enum):
    PYTHON = "python"

    # SQL dialects match the SQLFluff naming conventions.
    SQLITE = "sqlite"
    TSQL = "tsql"
    BIGQUERY = "bigquery"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    ANSI = "ansi"

    @classmethod
    def validate(cls, value: Union[str, Self]) -> Self:
        try:
            return cls(value)
        except ValueError:
            raise ValueError(
                f"Unsupported code language: {value}\n"
                f"Supported code languages: {', '.join([x.value for x in cls])}"
            )

    def is_sql_dialect(self) -> bool:
        return self in SQL_DIALECTS

    def to_syntax_lexer(self) -> str:
        """Convert the code language to a syntax lexer for Pygments.

        Reference: https://pygments.org/docs/lexers/
        """
        if self == CodeLang.PYTHON:
            return "python"
        elif self == CodeLang.SQLITE:
            return "sql"
        elif self == CodeLang.ANSI:
            return "sql"
        elif self == CodeLang.TSQL:
            return "tsql"
        elif self == CodeLang.BIGQUERY:
            return "sql"
        elif self == CodeLang.MYSQL:
            return "mysql"
        elif self == CodeLang.POSTGRES:
            return "postgres"
        else:
            raise ValueError(f"Unsupported code language: {self}")


SQL_DIALECTS = {
    CodeLang.SQLITE,
    CodeLang.TSQL,
    CodeLang.BIGQUERY,
    CodeLang.MYSQL,
    CodeLang.POSTGRES,
    CodeLang.ANSI,
}


class EvaluationOutputs(BaseModel):
    results: dict
    dataset_overview_statistics: dict


class ValidatorType(str, Enum):
    CODE = "code"


class SeedColumn(BaseModel):
    name: str
    description: Optional[str] = None

    @classmethod
    def from_dicts(cls, seeds: list[dict]) -> list[Self]:
        return [cls(**seed) for seed in seeds]


class SeedSubcategory(SeedColumn):
    num_new_values_to_generate: Optional[int] = Field(
        default=None, gt=0, le=MAX_NUM_DATA_SEED_VALUES
    )
    generated_values: dict[str, list[SeedValueT]] = {}
    values: dict[str, list[SeedValueT]] = {}

    @property
    def needs_generation(self) -> bool:
        return self.num_new_values_to_generate is not None and (
            len(self.generated_values) == 0
            or any(
                len(x) < self.num_new_values_to_generate
                for x in list(self.generated_values.values())
            )
        )

    @model_validator(mode="after")
    def check_values(self) -> Self:
        if self.num_new_values_to_generate is None and len(self.values) == 0:
            raise ValueError(
                "'values' cannot be empty when 'num_new_values_to_generate' is not provided."
            )
        return self


class SeedCategory(SeedColumn):
    values: list[SeedValueT] = Field(default=[])
    weights: list[float] = Field(default=[])
    num_new_values_to_generate: Optional[int] = Field(
        default=None, gt=0, le=MAX_NUM_DATA_SEED_VALUES
    )
    subcategories: Annotated[
        list[SeedSubcategory],
        Len(max_length=MAX_NUM_NESTED_DATA_SEED_SUBCATEGORIES),
    ] = []
    quality_rank: Optional[int] = None
    generated_values: list[SeedValueT] = []

    @property
    def has_subcategories(self) -> bool:
        return len(self.subcategories) > 0

    @property
    def needs_generation(self) -> bool:
        return (
            self.num_new_values_to_generate is not None
            and self.num_new_values_to_generate > len(self.generated_values)
        )

    @model_validator(mode="after")
    def check_values(self) -> Self:
        if self.num_new_values_to_generate is None and len(self.values) == 0:
            raise ValueError(
                "'values' cannot be empty when 'num_new_values_to_generate' is not provided."
            )
        return self

    @model_validator(mode="after")
    def check_values_in_subcategories(self) -> Self:
        if self.has_subcategories:
            parent_values = set(self.values)
            parent_values_sorted_list = sorted(list(parent_values))
            for subcategory in self.subcategories:
                if len(subcategory.values) > 0:
                    subcategories_keys = set(subcategory.values.keys())
                    if not subcategories_keys.issubset(parent_values):
                        raise ValueError(
                            f"Subcategory '{subcategory.name}' must only have values for {parent_values_sorted_list}."
                        )
                    if subcategory.num_new_values_to_generate is None:
                        if parent_values != subcategories_keys:
                            raise ValueError(
                                f"Subcategory '{subcategory.name}' must have values "
                                f"for all of these categories: {parent_values_sorted_list}."
                            )
                        keys_with_no_values = []
                        for key, value in subcategory.values.items():
                            if len(value) == 0:
                                keys_with_no_values.append(key)
                        if len(keys_with_no_values) > 0:
                            raise ValueError(
                                f"Values provided for categories {keys_with_no_values} under "
                                f"subcategory '{subcategory.name}' cannot be empty."
                            )
        return self

    @model_validator(mode="after")
    def check_dynamic_categories_have_dynamic_subcategories(self) -> Self:
        if self.num_new_values_to_generate is not None and self.has_subcategories:
            invalid_subcategories = []
            for subcategory in self.subcategories:
                if subcategory.num_new_values_to_generate is None:
                    invalid_subcategories.append(subcategory.name)
            if len(invalid_subcategories) > 0:
                raise ValueError(
                    f"Subcategories {invalid_subcategories} must also have 'num_new_values_to_generate' "
                    f"provided when 'num_new_values_to_generate' for the parent category '{self.name}' is provided."
                )
        return self


class CategoricalDataSeeds(BaseModel):
    seed_categories: list[SeedCategory]

    dataset_schema_map: Optional[dict] = None

    @property
    def needs_generation(self) -> bool:
        return any(cat.needs_generation for cat in self.seed_categories) or any(
            sub_cat.needs_generation
            for cat in self.seed_categories
            for sub_cat in cat.subcategories
        )

    def add(self, seed_cateories: list[SeedCategory]) -> None:
        self.seed_categories.extend(seed_cateories)

    def inspect(self) -> None:
        """Pretty print the seed categories and their values."""
        columns_to_print = [
            "name",
            "description",
            "values",
            "subcategories",
        ]
        console = Console()
        render_list = []
        title = Text(
            "-" * 80 + "\n🌱 Categorical Seed Columns \n" + "-" * 80 + "\n",
            style="bold",
        )

        render_list.append(title)
        for seed in self.seed_categories:
            seed = seed.model_dump(include=columns_to_print)
            render_list.append(Pretty(seed))

        console.print(Group(*render_list))

    def __repr__(self) -> str:
        tab = "    "
        seed_categories = f"{tab}seed_categories:\n"
        for cat in self.seed_categories:
            needs_gen = " (needs generation)" if cat.needs_generation else ""
            seed_categories += f"{2 * tab}{cat.name}{needs_gen}\n"
            if cat.has_subcategories:
                for sub_cat in cat.subcategories:
                    needs_gen = (
                        " (needs generation)" if sub_cat.needs_generation else ""
                    )
                    seed_categories += f"{2 * tab}  |-- {sub_cat.name}{needs_gen}\n"
        return f"CategoricalDataSeeds(\n{seed_categories})"
