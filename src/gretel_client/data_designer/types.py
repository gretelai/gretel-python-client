import json

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Optional, Type, TypeAlias, TypeVar

import pandas as pd

from pydantic import BaseModel, Field, field_serializer, model_validator

from gretel_client.workflows.configs.tasks import (
    CodeLang,
    ConditionalDataColumn,
    DataConfig,
    GenerateColumnFromTemplate,
    JudgeWithLlm,
    OutputType,
    SamplingSourceType,
    SamplingStrategy,
)


class ModelSuite(str, Enum):
    APACHE_2_0 = "apache-2.0"
    LLAMA_3_x = "llama-3.x"


class ValidationType(str, Enum):
    CODE = "code"


class CodeValidatorSettings(BaseModel):
    code_lang: CodeLang
    target_columns: list[str]
    result_columns: list[str]

    def validate_columns(self, all_column_names: set[str]) -> None:
        if not set(self.target_columns).issubset(all_column_names):
            raise ValueError(
                "`code_columns` contains columns that have not been defined."
                f"\n* Available columns: {all_column_names}"
            )


class CodeValidator(BaseModel):
    settings: CodeValidatorSettings
    type: ValidationType = ValidationType.CODE


ValidatorT = TypeVar("ValidatorT", bound=CodeValidator)


class LLMJudgePromptTemplateType(str, Enum):
    # TODO: eliminate to use new judge task format
    TEXT_TO_PYTHON = "text_to_python"
    TEXT_TO_SQL = "text_to_sql"


class EvaluateDatasetSettings(BaseModel):
    ordered_list_like_columns: list[str] = Field(default_factory=list)
    list_like_columns: list[str] = Field(default_factory=list)
    llm_judge_column: str = Field(default="")
    columns_to_ignore: list[str] = Field(default_factory=list)


class EvaluationType(str, Enum):
    GENERAL = "general"


class Evaluator(BaseModel):
    settings: BaseModel
    type: EvaluationType


class GeneralDatasetEvaluator(Evaluator):
    settings: EvaluateDatasetSettings
    type: EvaluationType = EvaluationType.GENERAL


EvaluatorT: TypeAlias = GeneralDatasetEvaluator


class ContentType(BaseModel): ...


class TextContentType(ContentType): ...


class CodeContentType(ContentType):
    syntax: str


class StructuredContentType(ContentType):
    model: Annotated[Type[BaseModel] | None, Field(exclude=True)] = None
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


_CONTENT_TYPES = {
    OutputType.CODE: CodeContentType,
    OutputType.TEXT: TextContentType,
    OutputType.STRUCTURED: StructuredContentType,
}


class ColumnDataConfig(DataConfig):

    @model_validator(mode="before")
    @classmethod
    def populate_params(cls, data: dict) -> dict:
        t, p = data.get("type"), data.get("params", {})
        if not t:
            raise KeyError("No type specified.")

        if t not in _CONTENT_TYPES:
            raise ValueError(f"Unknown column type {t}")

        data["params"] = _CONTENT_TYPES[t].model_validate(p).model_dump()
        return data

    @field_serializer("type")
    def serialize_type(self, type: OutputType, _) -> str:
        return str(type.value)


class NonSamplingSupportedTypes(str, Enum):
    LLM_GENERATED = "llm-generated"
    VALIDATION = "validation"
    LLM_JUDGE = "llm-judge"


SamplingSupportedTypesT: TypeAlias = SamplingSourceType


SupportedColumnTypesT: TypeAlias = SamplingSourceType | NonSamplingSupportedTypes


DataColumnFromSamplingT: TypeAlias = ConditionalDataColumn


class DataColumnFromPrompt(GenerateColumnFromTemplate):
    type: NonSamplingSupportedTypes = NonSamplingSupportedTypes.LLM_GENERATED
    data_config: ColumnDataConfig = Field(
        default_factory=lambda: ColumnDataConfig(type=OutputType.TEXT)
    )


class DataColumnFromJudge(JudgeWithLlm):
    type: NonSamplingSupportedTypes = NonSamplingSupportedTypes.LLM_JUDGE
    name: str

    @model_validator(mode="before")
    @classmethod
    def populate_result_column(cls, data: dict) -> dict:
        data["result_column"] = data["name"]
        return data


DataColumnT: TypeAlias = (
    DataColumnFromSamplingT | DataColumnFromPrompt | DataColumnFromJudge
)


@dataclass
class DataPipelineMetadata:
    """Specification for dataset created by DataDesigner.

    We pass this object around to enable streamlined helper methods like
    `display_sample_record`, `fetch_dataset`, and `download_evaluation_report`.
    """

    sampling_based_columns: list[str]
    prompt_based_columns: list[str]
    llm_judge_columns: list[str] | None = None
    validation_columns: list[str] | None = None
    evaluation_columns: list[str] | None = None
    code_column_names: list[str] | None = None
    code_lang: CodeLang | None = None
    eval_type: LLMJudgePromptTemplateType | None = None


TaskOutputT = pd.DataFrame | dict


@dataclass(frozen=True)
class JudgeRubric:
    # When adding new rubrics, ensure all rubric names are capitalized to match jarvis
    text_to_python = {
        "Relevance": "Adherence to INSTRUCTIONS",
        "Readability": "Readability and Maintainability (Is the Python code easy to understand and maintain?)",
        "Efficiency": "Efficiency and Performance (Is the code optimized for performance?)",
        "Pythonic": "Pythonic Code and Best Practices (Does the code follow Python conventions and best practices?)",
    }

    text_to_sql = {
        "Relevance": "Adherence to INSTRUCTIONS and CONTEXT",
        "Readability": "Readability and Maintainability (Is the SQL code easy to understand and maintain?)",
        "Scalability": "Scalability (Does the solution scale well with larger datasets or more complex queries?)",
        "Standards": "Compliance with Standards (Does the SQL query follow SQL standards and best practices?)",
    }

    @classmethod
    def get_rubric(cls, eval_type: LLMJudgePromptTemplateType) -> dict[str, str]:
        if eval_type == LLMJudgePromptTemplateType.TEXT_TO_PYTHON:
            return cls.text_to_python
        elif eval_type == LLMJudgePromptTemplateType.TEXT_TO_SQL:
            return cls.text_to_sql
        else:
            raise ValueError(f"Unsupported judge template type: {eval_type}")


class SeedDataset(BaseModel):
    file_id: str
    sampling_strategy: SamplingStrategy = SamplingStrategy.ORDERED
    with_replacement: bool = False
