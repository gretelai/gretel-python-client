# needed for sphinx autodoc_type_aliases
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Self, TypeAlias
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
from gretel_client.workflows.configs.tasks import (
    ModelAlias,
    OutputType,
    SerializableConditionalDataColumn,
)

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
    LLM_TEXT = "llm-text"
    LLM_CODE = "llm-code"
    LLM_STRUCTURED = "llm-structured"
    LLM_JUDGE = "llm-judge"
    CODE_VALIDATION = "code-validation"
    EXPRESSION = "expression"


class Person(BaseModel):
    first_name: str
    middle_name: str | None
    last_name: str
    sex: Literal["Male", "Female"]
    age: int
    zipcode: str = Field(alias="postcode")
    street_number: int | str
    street_name: str
    unit: str
    city: str
    state: str | None = Field(alias="region")
    county: str | None = Field(alias="district")
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


class EvaluateDataDesignerDatasetSettings(AIDDConfigBase):
    llm_judge_column: str = ""
    columns_to_ignore: list[str] = Field(default_factory=list)
    validation_columns: list[str] = Field(default_factory=list)
    defined_categorical_columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_for_llm_judge_columns(self) -> Self:
        if self.llm_judge_column != "":
            self.columns_to_ignore.append("judged_by_llm")
        return self


class GeneralDatasetEvaluation(AIDDConfigBase):
    settings: EvaluateDataDesignerDatasetSettings = Field(
        default_factory=EvaluateDataDesignerDatasetSettings
    )
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


class SamplerColumn(WithPrettyRepr, tasks.ConditionalDataColumn):
    """AIDD column that uses a sampler to generate data.

    Sampler columns can be conditioned on other sampler columns using the `conditional_params` argument,
    which is a dictionary of conditions and parameters. Conditions are specified as strings involving
    the names of other sampler columns and the operators `==`, `!=`, `>`, `>=`, `<`, `<=`.

    Args:
        name: Name of the column.
        type: Type of sampler to use.
        params: Parameters for the sampler. If conditional_params are provided,
            these parameters will be used as the default when no condition is met.
        conditional_params: Conditional parameters for the sampler. The keys of the
            dict are conditions from other columns, and the values are the parameters
            for the sampler.
        convert_to: Optional data conversion to apply to the generated data. For
            numerical columns this can be "int" or "float", and for datetime columns,
            this can be a datetime format string (e.g. "%Y/%m/%d").

    Example::

        from gretel_client.navigator_client import Gretel
        from gretel_client.data_designer.columns import SamplerColumn
        from gretel_client.data_designer.params import (
            GaussianSamplerParams,
            CategorySamplerParams,
            SamplerType,
        )

        aidd = Gretel(api_key="prompt").data_designer.new()

        aidd.add_column(
            SamplerColumn(
                name="age",
                type=SamplerType.GAUSSIAN,
                params=GaussianSamplerParams(mean=35, stddev=5),
                convert_to="int",
            )
        )

        aidd.add_column(
            SamplerColumn(
                name="pet_type",
                type=SamplerType.CATEGORY,
                params=CategorySamplerParams(values=["dog", "cat", "bird"]),
                conditional_params={
                    "age < 20": CategorySamplerParams(values=["rabbit", "hamster"]),
                }
            )
        )
    """

    def pack(self) -> SerializableConditionalDataColumn:
        col_dict = self.model_dump()
        col_dict["sampling_type"] = col_dict.pop("type")
        return SerializableConditionalDataColumn(**col_dict)

    @classmethod
    def unpack(cls, column: SerializableConditionalDataColumn | dict) -> Self:
        """This can be used to unpack the true base type."""
        col_dict = (
            column.model_dump()
            if isinstance(column, SerializableConditionalDataColumn)
            else {**column}
        )
        col_dict["type"] = col_dict.pop("sampling_type")
        return cls(**col_dict)


class LLMGenColumn(
    WithPrettyRepr, tasks.GenerateColumnFromTemplateV2, WithDAGColumnMixin
):

    @model_validator(mode="before")
    @classmethod
    def _set_output_format(cls, data: Any) -> Any:
        if "output_format" not in data:
            return data

        if isinstance(data["output_format"], type) and issubclass(
            data["output_format"], BaseModel
        ):
            data["output_format"] = data["output_format"].model_json_schema()

        return data

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.prompt))

    @property
    def step_name(self) -> str:
        return f"generating-{OutputType(self.output_type).value}-column-{self.name}"

    @model_validator(mode="after")
    def assert_prompt_valid_jinja(self) -> Self:
        assert_valid_jinja2_template(self.prompt)
        return self

    ## Need to set exclude_unset=False in order to preserve
    ## the default values for these columns that otherwise get stripped
    ## when sending this to API.
    def model_dump(self, **kwargs) -> dict:
        kwargs.setdefault("exclude_unset", False)
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs) -> str:
        kwargs.setdefault("exclude_unset", False)
        return super().model_dump_json(**kwargs)

    def to_specific_column_type(self):
        if self.output_type == OutputType.TEXT:
            return LLMTextColumn(**self.model_dump())
        elif self.output_type == OutputType.CODE:
            return LLMCodeColumn(**self.model_dump())
        elif self.output_type == OutputType.STRUCTURED:
            return LLMStructuredColumn(**self.model_dump())
        else:
            raise NotImplementedError(f"Unknown output type: {self.output_type}")


class LLMTextColumn(LLMGenColumn):
    """AIDD column that uses an LLM to generate text.

    Args:
        name: Name of the column.
        prompt: Prompt template to use for generation.
        system_prompt: System prompt for the LLM. Useful for defining the LLM's role,
            tone, and other instructions. However, do not provide any instructions
            related to the output format, as this is handled internally by AIDD.
        model_alias: Model alias to use for the LLM. Defaults to `ModelAlias.TEXT`.
    """

    model_alias: str | ModelAlias = Field(default=ModelAlias.TEXT)
    output_type: OutputType = Field(default=OutputType.TEXT)


class LLMCodeColumn(LLMGenColumn):
    """AIDD column that uses an LLM to generate code.

    Args:
        name: Name of the column.
        prompt: Prompt template to use for generation.
        system_prompt: System prompt for the LLM. Useful for defining the LLM's role,
            tone, and other instructions. However, do not provide any instructions
            related to the output format, as this is handled internally by AIDD.
        model_alias: Model alias to use for the LLM. Defaults to `ModelAlias.CODE`.
    """

    model_alias: str | ModelAlias = Field(default=ModelAlias.CODE)
    output_type: OutputType = Field(default=OutputType.CODE)


class LLMStructuredColumn(LLMGenColumn):
    """AIDD column that uses an LLM to generate structured data.

    Args:
        name: Name of the column.
        prompt: Prompt template to use for generation.
        system_prompt: System prompt for the LLM. Useful for defining the LLM's role,
            tone, and other instructions. However, do not provide any instructions
            related to the output format, as this is handled internally by AIDD.
        model_alias: Model alias to use for the LLM. Defaults to `ModelAlias.STRUCTURED`.
    """

    model_alias: str | ModelAlias = Field(default=ModelAlias.STRUCTURED)
    output_type: OutputType = Field(default=OutputType.STRUCTURED)


class LLMJudgeColumn(WithPrettyRepr, tasks.JudgeWithLlm, WithDAGColumnMixin):
    """AIDD column for llm-as-a-judge with custom rubrics.

    Args:
        name: Name of the column.
        prompt: Prompt template to use for llm-as-a-judge.
        rubrics: List of rubrics to use for evaluation.
        num_samples_to_judge: Number of samples to judge. If None, the full dataset
            will be judged. If less than the total number of rows in the dataset,
            a random sample of the specified size will be judged.
        model_alias: Model alias to use for the LLM. Defaults to `ModelAlias.JUDGE`.
    """

    result_column: str = Field(..., alias="name")

    @property
    def name(self) -> str:
        return self.result_column

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.prompt))

    @property
    def step_name(self) -> str:
        return f"using-llm-to-judge-column-{self.name}"


class CodeValidationColumn(WithPrettyRepr, AIDDConfigBase, WithDAGColumnMixin):
    """AIDD column for validating code in another column.

    Code validation is currently supported for Python and SQL.

    Args:
        name: Name of the column.
        code_lang: Language of the code to validate.
        target_column: Column with code to validate.
    """

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

    @property
    def step_name(self) -> str:
        return f"validating-code-in-column-{self.target_column}"


class ExpressionColumn(
    WithPrettyRepr, tasks.GenerateColumnFromExpression, WithDAGColumnMixin
):
    """AIDD column for generated data based on jinja2 expressions.

    Args:
        name: Name of the column.
        expr: Expression to use for generation.
        dtype: Data type of the column. Can be "str" (default), "int",
            "float", or "bool".
    """

    @property
    def required_columns(self) -> list[str]:
        return list(get_prompt_template_keywords(self.expr))

    @model_validator(mode="after")
    def assert_expression_valid_jinja(self) -> Self:
        assert_valid_jinja2_template(self.expr)
        return self

    @property
    def step_name(self) -> str:
        return f"rendering-expression-column-{self.name}"


class DataSeedColumn(WithPrettyRepr, AIDDConfigBase):
    """Column in a seed dataset.

    This object is meant for internal bookkeeping and should not be used directly.

    Args:
        name: Name of the column.
        file_id: File ID of the seed dataset.
    """

    name: str
    file_id: str


##########################################################
# Type aliases
##########################################################

DAGColumnT: TypeAlias = (
    LLMGenColumn
    | LLMTextColumn
    | LLMCodeColumn
    | LLMStructuredColumn
    | LLMJudgeColumn
    | CodeValidationColumn
    | ExpressionColumn
)
AIDDColumnT: TypeAlias = SamplerColumn | DAGColumnT
MagicColumnT: TypeAlias = AIDDColumnT | DataSeedColumn
ColumnProviderTypeT: TypeAlias = tasks.SamplerType | ProviderType
EvaluationReportT: TypeAlias = GeneralDatasetEvaluation
TaskOutputT: TypeAlias = pd.DataFrame | dict
