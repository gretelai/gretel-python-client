import json

from enum import Enum
from typing import Literal, Self, TypeAlias
from uuid import UUID

import pandas as pd

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gretel_client.data_designer.constants import (
    SQL_DIALECTS,
    VALIDATE_PYTHON_COLUMN_SUFFIXES,
    VALIDATE_SQL_COLUMN_SUFFIXES,
)
from gretel_client.data_designer.utils import (
    assert_valid_jinja2_template,
    get_prompt_template_keywords,
    WithPrettyRepr,
)
from gretel_client.workflows.configs import tasks

##########################################################
# Enums
##########################################################


class ModelSuite(str, Enum):
    APACHE_2_0 = "apache-2.0"
    LLAMA_3_x = "llama-3.x"


class LLMJudgePromptTemplateType(str, Enum):
    # TODO: eliminate to use new judge task format
    TEXT_TO_PYTHON = "text_to_python"
    TEXT_TO_SQL = "text_to_sql"


class EvaluationType(str, Enum):
    GENERAL = "general"


class ProviderType(str, Enum):
    LLM_GEN = "llm-gen"
    LLM_JUDGE = "llm-judge"
    CODE_VALIDATION = "code-validation"
    EXPRESSION = "expression"


class Person(BaseModel):
    first_name: str
    middle_name: str | None
    last_name: str
    sex: Literal["Male", "Female"]
    age: int
    postcode: str = Field(alias="zipcode")
    street_number: int | str
    street_name: str
    unit: str
    city: str
    region: str | None = Field(alias="state")
    district: str | None = Field(alias="county")
    country: str
    ethnic_background: str | None
    marital_status: str | None
    education_level: str | None
    bachelors_field: str | None
    occupation: str | None
    uuid: UUID
    locale: str = "en_US"


##########################################################
# Helper task configurations
##########################################################


class AIDDConfigBase(BaseModel):
    model_config = ConfigDict(
        protected_namespaces=(),
        extra="allow",
        validate_default=False,
    )


class EvaluateDatasetSettings(AIDDConfigBase):
    ordered_list_like_columns: list[str] = Field(default_factory=list)
    list_like_columns: list[str] = Field(default_factory=list)
    columns_to_ignore: list[str] = Field(default_factory=list)


class GeneralDatasetEvaluation(AIDDConfigBase):
    settings: EvaluateDatasetSettings = Field(default_factory=EvaluateDatasetSettings)
    type: EvaluationType = EvaluationType.GENERAL


class SeedDataset(AIDDConfigBase):
    file_id: str
    sampling_strategy: tasks.SamplingStrategy = tasks.SamplingStrategy.ORDERED
    with_replacement: bool = False


##########################################################
# AIDD data column types
##########################################################


class WithDAGColumnMixin:
    @property
    def required_columns(self) -> list[str]:
        return []

    @property
    def side_effect_columns(self) -> list[str]:
        return []


class SamplerColumn(WithPrettyRepr, tasks.ConditionalDataColumn): ...


class LLMGenColumn(
    WithPrettyRepr, tasks.GenerateColumnFromTemplate, WithDAGColumnMixin
):
    data_config: tasks.DataConfig = Field(
        default_factory=lambda: tasks.DataConfig(type=tasks.OutputType.TEXT, params={})
    )

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.prompt))

    @model_validator(mode="after")
    def assert_prompt_valid_jinja(self) -> Self:
        assert_valid_jinja2_template(self.prompt)
        return self

    @model_validator(mode="after")
    def validate_data_config_params(self) -> Self:
        if self.data_config.type == tasks.OutputType.STRUCTURED:
            if "model" in self.data_config.params:
                model = self.data_config.params.pop("model")
                self.data_config.params["json_schema"] = model.model_json_schema()
            elif isinstance(self.data_config.params.get("json_schema"), str):
                self.data_config.params["json_schema"] = json.loads(
                    self.data_config.params["json_schema"]
                )
        elif self.data_config.type == tasks.OutputType.CODE:
            if "syntax" not in self.data_config.params:
                raise ValueError("Missing `syntax` parameter for code column.")
        return self


class LLMJudgeColumn(WithPrettyRepr, tasks.JudgeWithLlm, WithDAGColumnMixin):
    result_column: str = Field(..., alias="name")

    @property
    def name(self) -> str:
        return self.result_column

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.prompt))


class CodeValidationColumn(WithPrettyRepr, AIDDConfigBase, WithDAGColumnMixin):
    name: str
    code_lang: tasks.CodeLang
    target_column: str

    @property
    def required_columns(self) -> list[str]:
        return [self.target_column]

    @property
    def side_effect_columns(self) -> list[str]:
        suffixes = (
            VALIDATE_SQL_COLUMN_SUFFIXES
            if self.code_lang in SQL_DIALECTS
            else VALIDATE_PYTHON_COLUMN_SUFFIXES
        )
        columns = []
        for suffix in suffixes:
            columns.append(f"{self.target_column}{suffix}")
        return columns


class ExpressionColumn(
    WithPrettyRepr, tasks.GenerateColumnFromExpression, WithDAGColumnMixin
):

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.expr))

    @model_validator(mode="after")
    def assert_expression_valid_jinja(self) -> Self:
        assert_valid_jinja2_template(self.expr)
        return self


class DataSeedColumn(WithPrettyRepr, AIDDConfigBase):
    name: str
    file_id: str


##########################################################
# Type aliases
##########################################################

DAGColumnT: TypeAlias = (
    LLMGenColumn | LLMJudgeColumn | CodeValidationColumn | ExpressionColumn
)
AIDDColumnT: TypeAlias = SamplerColumn | DAGColumnT
MagicColumnT: TypeAlias = AIDDColumnT | DataSeedColumn
ColumnProviderTypeT: TypeAlias = tasks.SamplingSourceType | ProviderType
EvaluationReportT: TypeAlias = GeneralDatasetEvaluation
TaskOutputT: TypeAlias = pd.DataFrame | dict
