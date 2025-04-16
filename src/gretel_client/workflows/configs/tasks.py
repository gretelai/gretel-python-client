# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Union

from pydantic import Field, RootModel

from gretel_client.workflows.configs.base import ConfigBase


class AzureDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    container: Annotated[str, Field(title="Container")]


class AzureSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    container: Annotated[str, Field(title="Container")]


class Mode(str, Enum):
    APPEND = "append"
    REPLACE = "replace"


class DestinationSyncConfig(ConfigBase):
    mode: Annotated[Optional[Mode], Field(title="Mode")] = "replace"


class BigqueryDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None
    bq_dataset: Annotated[Optional[str], Field(title="Bq Dataset")] = None


class Query(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    query: Annotated[str, Field(title="Query")]


class BigquerySource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class Combiner(ConfigBase):
    pass


class ConcatDatasets(ConfigBase):
    pass


class DataSource(ConfigBase):
    data_source: Annotated[str, Field(title="Data Source")]


class DatabricksDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None
    volume: Annotated[Optional[str], Field(title="Volume")] = (
        "gretel_databricks_connector"
    )


class DatabricksSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class DropColumns(ConfigBase):
    columns: Annotated[List[str], Field(title="Columns")]


class DummyTaskWithInputs(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100


class DummyTaskWithListOfInputs(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100


class DistributionType(str, Enum):
    UNIFORM = "uniform"
    MANUAL = "manual"


class ManualDistributionParams(ConfigBase):
    values: Annotated[List[float], Field(min_length=1, title="Values")]
    weights: Annotated[Optional[List[float]], Field(title="Weights")] = None


class ModelAlias(str, Enum):
    TEXT = "text"
    CODE = "code"
    JUDGE = "judge"
    STRUCTURED = "structured"


class UniformDistributionParams(ConfigBase):
    low: Annotated[float, Field(title="Low")]
    high: Annotated[float, Field(title="High")]


class EvaluateDataset(ConfigBase):
    seed_columns: Annotated[List[str], Field(title="Seed Columns")]
    ordered_list_like_columns: Annotated[
        Optional[List[str]], Field(title="Ordered List Like Columns")
    ] = None
    other_list_like_columns: Annotated[
        Optional[List[str]], Field(title="Other List Like Columns")
    ] = None
    llm_judge_column: Annotated[Optional[str], Field(title="Llm Judge Column")] = ""
    columns_to_ignore: Annotated[
        Optional[List[str]], Field(title="Columns To Ignore")
    ] = None


class EvaluateSafeSyntheticsDataset(ConfigBase):
    skip_attribute_inference_protection: Annotated[
        Optional[bool], Field(title="Skip Attribute Inference Protection")
    ] = False
    attribute_inference_protection_quasi_identifier_count: Annotated[
        Optional[int],
        Field(gt=0, title="Attribute Inference Protection Quasi Identifier Count"),
    ] = 3
    skip_membership_inference_protection: Annotated[
        Optional[bool], Field(title="Skip Membership Inference Protection")
    ] = False
    membership_inference_protection_column_name: Annotated[
        Optional[str], Field(title="Membership Inference Protection Column Name")
    ] = None
    skip_pii_replay: Annotated[Optional[bool], Field(title="Skip Pii Replay")] = False
    pii_replay_entities: Annotated[
        Optional[List[str]], Field(title="Pii Replay Entities")
    ] = None
    pii_replay_columns: Annotated[
        Optional[List[str]], Field(title="Pii Replay Columns")
    ] = None


class SystemPromptType(str, Enum):
    REFLECTION = "reflection"
    COGNITION = "cognition"
    COLUMN_CLASSIFICATION = "column_classification"


class ExtractDataSeedsFromSampleRecords(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    sample_records: Annotated[List[Dict[str, Any]], Field(title="Sample Records")]
    max_num_seeds: Annotated[
        Optional[int], Field(ge=1, le=10, title="Max Num Seeds")
    ] = 5
    num_assistants: Annotated[
        Optional[int], Field(ge=1, le=8, title="Num Assistants")
    ] = 5
    dataset_context: Annotated[Optional[str], Field(title="Dataset Context")] = ""
    system_prompt_type: Optional[SystemPromptType] = "cognition"
    num_samples: Annotated[Optional[int], Field(title="Num Samples")] = 25


class GcsDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class GcsSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class OutputType(str, Enum):
    CODE = "code"
    TEXT = "text"
    STRUCTURED = "structured"


class Dtype(str, Enum):
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BOOL = "bool"


class GenerateColumnFromExpression(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    expr: Annotated[str, Field(title="Expr")]
    dtype: Annotated[Optional[Dtype], Field(title="Dtype")] = "str"


class DataConfig(ConfigBase):
    type: Optional[OutputType] = "text"
    params: Annotated[Optional[Dict[str, Any]], Field(title="Params")] = None


class BernoulliMixtureSamplerParams(ConfigBase):
    p: Annotated[
        float,
        Field(
            description="Bernoulli distribution probability of success.",
            ge=0.0,
            le=1.0,
            title="P",
        ),
    ]
    dist_name: Annotated[
        str,
        Field(
            description="Mixture distribution name. Samples will be equal to the distribution sample with probability `p`, otherwise equal to 0. Must be a valid scipy.stats distribution name.",
            title="Dist Name",
        ),
    ]
    dist_params: Annotated[
        Dict[str, Any],
        Field(
            description="Parameters of the scipy.stats distribution given in `dist_name`.",
            title="Dist Params",
        ),
    ]


class BernoulliSamplerParams(ConfigBase):
    p: Annotated[
        float, Field(description="Probability of success.", ge=0.0, le=1.0, title="P")
    ]


class BinomialSamplerParams(ConfigBase):
    n: Annotated[int, Field(description="Number of trials.", title="N")]
    p: Annotated[
        float,
        Field(
            description="Probability of success on each trial.",
            ge=0.0,
            le=1.0,
            title="P",
        ),
    ]


class CategorySamplerParams(ConfigBase):
    values: Annotated[
        List[Union[str, int, float]],
        Field(
            description="List of possible categorical values that can be sampled from.",
            min_length=1,
            title="Values",
        ),
    ]
    weights: Annotated[
        Optional[List[float]],
        Field(
            description="List of unnormalized probability weights to assigned to each value, in order. Larger values will be sampled with higher probability.",
            title="Weights",
        ),
    ] = None


class ConstraintType(str, Enum):
    SCALAR_INEQUALITY = "scalar_inequality"
    COLUMN_INEQUALITY = "column_inequality"


class Unit(str, Enum):
    Y = "Y"
    M = "M"
    D = "D"
    H = "h"
    m_1 = "m"
    S = "s"


class DatetimeSamplerParams(ConfigBase):
    start: Annotated[
        str,
        Field(
            description="Earliest possible datetime for sampling range, inclusive.",
            title="Start",
        ),
    ]
    end: Annotated[
        str,
        Field(
            description="Latest possible datetime for sampling range, inclusive.",
            title="End",
        ),
    ]
    unit: Annotated[
        Optional[Unit],
        Field(
            description="Sampling units, e.g. the smallest possible time interval between samples.",
            title="Unit",
        ),
    ] = "D"


class GaussianSamplerParams(ConfigBase):
    mean: Annotated[
        float, Field(description="Mean of the Gaussian distribution", title="Mean")
    ]
    stddev: Annotated[
        float,
        Field(
            description="Standard deviation of the Gaussian distribution",
            title="Stddev",
        ),
    ]


class InequalityOperator(str, Enum):
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"


class Sex(str, Enum):
    MALE = "Male"
    FEMALE = "Female"


class PersonSamplerParams(ConfigBase):
    locale: Annotated[
        Optional[str],
        Field(
            description="Locale string, determines the language and geographic locale that a synthetic person will be sampled from. E.g, en_US, en_GB, fr_FR, ...",
            title="Locale",
        ),
    ] = "en_US"
    sex: Annotated[
        Optional[Sex],
        Field(
            description="If specified, then only synthetic people of the specified sex will be sampled.",
            title="Sex",
        ),
    ] = None
    city: Annotated[
        Optional[Union[str, List[str]]],
        Field(
            description="If specified, then only synthetic people from these cities will be sampled.",
            title="City",
        ),
    ] = None
    age_range: Annotated[
        Optional[List[int]],
        Field(
            description="If specified, then only synthetic people within this age range will be sampled.",
            max_length=2,
            min_length=2,
            title="Age Range",
        ),
    ] = [18, 114]
    state: Annotated[
        Optional[Union[str, List[str]]],
        Field(
            description="Only supported for 'en_US' locale. If specified, then only synthetic people from these states will be sampled. States must be given as two-letter abbreviations.",
            title="State",
        ),
    ] = None


class PoissonSamplerParams(ConfigBase):
    mean: Annotated[
        float,
        Field(description="Mean number of events in a fixed interval.", title="Mean"),
    ]


class SamplerType(str, Enum):
    BERNOULLI = "bernoulli"
    BERNOULLI_MIXTURE = "bernoulli_mixture"
    BINOMIAL = "binomial"
    CATEGORY = "category"
    DATETIME = "datetime"
    GAUSSIAN = "gaussian"
    PERSON = "person"
    POISSON = "poisson"
    SCIPY = "scipy"
    SUBCATEGORY = "subcategory"
    TIMEDELTA = "timedelta"
    UNIFORM = "uniform"
    UUID = "uuid"


class ScipySamplerParams(ConfigBase):
    dist_name: Annotated[
        str, Field(description="Name of a scipy.stats distribution.", title="Dist Name")
    ]
    dist_params: Annotated[
        Dict[str, Any],
        Field(
            description="Parameters of the scipy.stats distribution given in `dist_name`.",
            title="Dist Params",
        ),
    ]


class SubcategorySamplerParams(ConfigBase):
    category: Annotated[
        str,
        Field(
            description="Name of parent category to this subcategory.", title="Category"
        ),
    ]
    values: Annotated[
        Dict[str, List[Union[str, int, float]]],
        Field(
            description="Mapping from each value of parent category to a list of subcategory values.",
            title="Values",
        ),
    ]


class Unit1(str, Enum):
    D = "D"
    H = "h"
    M = "m"
    S = "s"


class TimeDeltaSamplerParams(ConfigBase):
    dt_min: Annotated[
        int,
        Field(
            description="Minimum possible time-delta for sampling range, inclusive. Must be less than `dt_max`.",
            ge=0,
            title="Dt Min",
        ),
    ]
    dt_max: Annotated[
        int,
        Field(
            description="Maximum possible time-delta for sampling range, exclusive. Must be greater than `dt_min`.",
            gt=0,
            title="Dt Max",
        ),
    ]
    reference_column_name: Annotated[
        str,
        Field(
            description="Name of an existing datetime column to condition time-delta sampling on.",
            title="Reference Column Name",
        ),
    ]
    unit: Annotated[
        Optional[Unit1],
        Field(
            description="Sampling units, e.g. the smallest possible time interval between samples.",
            title="Unit",
        ),
    ] = "D"


class UUIDSamplerParams(ConfigBase):
    prefix: Annotated[
        Optional[str],
        Field(description="String prepended to the front of the UUID.", title="Prefix"),
    ] = None
    short_form: Annotated[
        Optional[bool],
        Field(
            description="If true, all UUIDs sampled will be truncated at 8 characters.",
            title="Short Form",
        ),
    ] = False
    uppercase: Annotated[
        Optional[bool],
        Field(
            description="If true, all letters in the UUID will be capitalized.",
            title="Uppercase",
        ),
    ] = False


class UniformSamplerParams(ConfigBase):
    low: Annotated[
        float,
        Field(
            description="Lower bound of the uniform distribution, inclusive.",
            title="Low",
        ),
    ]
    high: Annotated[
        float,
        Field(
            description="Upper bound of the uniform distribution, inclusive.",
            title="High",
        ),
    ]


class GenerateDatasetFromSampleRecords(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    sample_records: Annotated[List[Dict[str, Any]], Field(title="Sample Records")]
    target_num_records: Annotated[
        Optional[int], Field(ge=50, le=10000, title="Target Num Records")
    ] = 500
    system_prompt_type: Optional[SystemPromptType] = "cognition"
    num_records_per_seed: Annotated[
        Optional[int], Field(ge=1, le=10, title="Num Records Per Seed")
    ] = 5
    append_seeds_to_dataset: Annotated[
        Optional[bool], Field(title="Append Seeds To Dataset")
    ] = True
    num_examples_per_prompt: Annotated[
        Optional[int], Field(ge=1, le=50, title="Num Examples Per Prompt")
    ] = 5
    dataset_context: Annotated[Optional[str], Field(title="Dataset Context")] = ""


class DataColumnFromSamplingConfig(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    sampling_type: Annotated[str, Field(title="Sampling Type")]
    params: Annotated[Dict[str, Any], Field(title="Params")]


class NumNewValuesToGenerate(RootModel[int]):
    root: Annotated[int, Field(gt=0, le=25, title="Num New Values To Generate")]


class SeedSubcategory(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    description: Annotated[Optional[str], Field(title="Description")] = None
    num_new_values_to_generate: Annotated[
        Optional[NumNewValuesToGenerate], Field(title="Num New Values To Generate")
    ] = None
    generated_values: Annotated[
        Optional[Dict[str, List[Union[str, int, bool]]]],
        Field(title="Generated Values"),
    ] = {}
    values: Annotated[
        Optional[Dict[str, List[Union[str, int, bool]]]], Field(title="Values")
    ] = {}


class GetGretelDataset(ConfigBase):
    name: Annotated[str, Field(title="Name")]


class Holdout(ConfigBase):
    holdout: Annotated[Optional[Union[float, int]], Field(title="Holdout")] = 0.05
    max_holdout: Annotated[Optional[int], Field(title="Max Holdout")] = 2000
    group_by: Annotated[Optional[str], Field(title="Group By")] = None


class IdGenerator(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100


class Rubric(ConfigBase):
    scoring: Annotated[
        Dict[str, str],
        Field(
            description="Dictionary specifying score: description pairs for rubric scoring.",
            title="Scoring",
        ),
    ]
    name: Annotated[
        str,
        Field(
            description="A clear, pythonic class name for this rubric.", title="Name"
        ),
    ]
    description: Annotated[
        Optional[str],
        Field(
            description="An informative and detailed assessment guide for using this rubric.",
            title="Description",
        ),
    ] = ""


class MssqlDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None


class MssqlSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class MysqlDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None


class MysqlSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class NameGenerator(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100
    column_name: Annotated[Optional[str], Field(title="Column Name")] = "name"
    seed: Annotated[Optional[int], Field(title="Seed")] = None
    should_fail: Annotated[Optional[bool], Field(title="Should Fail")] = False


class OracleDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None


class OracleSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class PostgresDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None


class PostgresSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class GenerateParams(ConfigBase):
    seed_records_multiplier: Annotated[
        Optional[int],
        Field(
            description="For conditional generation, emit this number of records consecutively for each prompt record in the seed dataset. Defaults to `1`, which means generating exactly one record per seed record. Ignored for unconditional generation.",
            gt=0,
            title="seed_records_multiplier",
        ),
    ] = 1
    maximum_text_length: Annotated[
        Optional[int],
        Field(
            description="Maximum number of tokens to generate (not including the prompt) in output text. Defaults to `42`.",
            gt=0,
            title="maximum_text_length",
        ),
    ] = 42
    top_p: Annotated[
        Optional[float],
        Field(
            description="Defaults to 0.8987. If set to float < 1, only the most probable tokens with probabilities that add up to ``top_p`` or higher are kept for generation.",
            ge=0.0,
            le=1.0,
            title="top_p",
        ),
    ] = 0.8987601335810778
    top_k: Annotated[
        Optional[int],
        Field(
            description="The number of highest probability vocabulary tokens to keep for top-k-filtering. Defaults to `43`.",
            ge=0,
            title="top_k",
        ),
    ] = 43
    num_beams: Annotated[
        Optional[int],
        Field(
            description="Number of beams for beam search. 1 means no beam search. Defaults to `1`.",
            ge=1,
            title="num_beams",
        ),
    ] = 1
    do_sample: Annotated[
        Optional[bool],
        Field(
            description="Whether or not to use sampling; use greedy decoding otherwise. Defaults to `True`.",
            title="do_sample",
        ),
    ] = True
    do_early_stopping: Annotated[
        Optional[bool],
        Field(
            description="Whether to stop the beam search when at least ``num_beams`` sentences are finished per batch or not. Defaults to `True`.",
            title="do_early_stopping",
        ),
    ] = True
    typical_p: Annotated[
        Optional[float],
        Field(
            description="The amount of probability mass from the original distribution that we wish to consider. Defaults to `0.8`.",
            ge=0.0,
            le=1.0,
            title="typical_p",
        ),
    ] = 0.8
    temperature: Annotated[
        Optional[float],
        Field(
            description="Passed as `temperature` argument to the generator. The value used to module the next token probabilities. It defaults to `None`, in which casemodels' default temperature is used or 1.0.",
            gt=0.0,
            title="temperature",
        ),
    ] = None
    use_vllm: Annotated[
        Optional[bool],
        Field(
            description="If True, load the generator using vLLM. Defaults to `False`.",
            title="use_vllm",
        ),
    ] = False


class PromptPretrainedModel(ConfigBase):
    pretrained_model: Annotated[
        Optional[str],
        Field(
            description="Select the text generation model to fine-tune from HuggingFace. Defaults to `meta-llama/Llama-3.1-8B-Instruct`.",
            title="Pretrained Model",
        ),
    ] = "meta-llama/Llama-3.1-8B-Instruct"
    prompt_template: Annotated[
        Optional[str],
        Field(
            description="All prompt inputs are formatted according to this template. The template must either start with '@' and reference the name of a pre-defined template, or contain a single '%s' formatting verb.",
            title="Prompt Template",
        ),
    ] = None
    generate: Optional[GenerateParams] = None


class S3Destination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class S3Source(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class SampleDataSeeds(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100


class SamplingStrategy(str, Enum):
    ORDERED = "ordered"
    SHUFFLE = "shuffle"


class SampleFromDataset(ConfigBase):
    num_samples: Annotated[Optional[int], Field(title="Num Samples")] = None
    strategy: Optional[SamplingStrategy] = "ordered"
    with_replacement: Annotated[Optional[bool], Field(title="With Replacement")] = False


class SeedFromRecords(ConfigBase):
    records: Annotated[List[Dict[str, Any]], Field(title="Records")]


class SnowflakeDestination(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    database: Annotated[Optional[str], Field(title="Database")] = None
    table: Annotated[str, Field(title="Table")]
    sync: Optional[DestinationSyncConfig] = None


class SnowflakeSource(ConfigBase):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class LRScheduler(str, Enum):
    COSINE = "cosine"
    LINEAR = "linear"
    COSINE_WITH_RESTARTS = "cosine_with_restarts"
    POLYNOMIAL = "polynomial"
    CONSTANT = "constant"
    CONSTANT_WITH_WARMUP = "constant_with_warmup"


class TabularFTGenerateStopParams(ConfigBase):
    patience: Annotated[
        Optional[int],
        Field(
            description="Number of consecutive generations where the `invalid_fraction_threshold` is reached before stopping generation.",
            ge=0,
            title="patience",
        ),
    ] = 3
    invalid_fraction_threshold: Annotated[
        Optional[float],
        Field(
            description="The fraction of invalid records that will stop generation after the `patience` limit is reached.",
            ge=0.0,
            le=1.0,
            title="invalid_fraction_threshold",
        ),
    ] = 0.8


class Delta(RootModel[float]):
    root: Annotated[
        float,
        Field(
            description="Probability of accidentally leaking information. Setting to 'auto' usesdelta of 1/n^1.2, where n is the number of training records. The value `1.2` is set in /python/src/gretel_skynet/runtime/model_auto_config/gpt_x.py:L15",
            gt=0.0,
            lt=1.0,
            title="delta",
        ),
    ]


class Delta1(str, Enum):
    AUTO = "auto"


class TabularFTPrivacyParams(ConfigBase):
    dp: Annotated[
        Optional[bool],
        Field(
            description="Enable differentially-private training with DP-SGD.",
            title="dp",
        ),
    ] = False
    epsilon: Annotated[
        Optional[float],
        Field(
            description="Target for epsilon when training completes.",
            ge=0.1,
            le=100.0,
            title="epsilon",
        ),
    ] = 8
    delta: Annotated[
        Optional[Union[Delta, Delta1]],
        Field(
            description="Probability of accidentally leaking information. Setting to 'auto' usesdelta of 1/n^1.2, where n is the number of training records. The value `1.2` is set in /python/src/gretel_skynet/runtime/model_auto_config/gpt_x.py:L15",
            title="delta",
        ),
    ] = "auto"
    per_sample_max_grad_norm: Annotated[
        Optional[float],
        Field(
            description="Maximum L2 norm of per sample gradients.",
            gt=0.0,
            title="per_sample_max_grad_norm",
        ),
    ] = 1.0


class NumInputRecordsToSample(RootModel[int]):
    root: Annotated[
        int,
        Field(
            description="Number of records the model will see during training. This parameter is a proxy for training time. For example, if its value is the same size as the input dataset, this is like training for a single epoch. If its value is larger, this is like training for multiple (possibly fractional) epochs. If its value is smaller, this is like training for a fraction of an epoch. Supports 'auto' where a reasonable value is chosen based on other config params and data.",
            gt=0,
            title="num_input_records_to_sample",
        ),
    ]


class NumInputRecordsToSample1(str, Enum):
    AUTO = "auto"


class TabularFTTrainingParams(ConfigBase):
    num_input_records_to_sample: Annotated[
        Optional[Union[NumInputRecordsToSample, NumInputRecordsToSample1]],
        Field(
            description="Number of records the model will see during training. This parameter is a proxy for training time. For example, if its value is the same size as the input dataset, this is like training for a single epoch. If its value is larger, this is like training for multiple (possibly fractional) epochs. If its value is smaller, this is like training for a fraction of an epoch. Supports 'auto' where a reasonable value is chosen based on other config params and data.",
            title="num_input_records_to_sample",
        ),
    ] = "auto"
    batch_size: Annotated[
        Optional[int],
        Field(
            description="The batch size per device for training",
            gt=0,
            title="batch_size",
        ),
    ] = 1
    gradient_accumulation_steps: Annotated[
        Optional[int],
        Field(
            description="Number of update steps to accumulate the gradients for, before performing a backward/update pass. This technique increases the effective batch size that will fit into GPU memory.",
            gt=0,
            title="gradient_accumulation_steps",
        ),
    ] = 8
    weight_decay: Annotated[
        Optional[float],
        Field(
            description="The weight decay to apply (if not zero) to all layers except all bias and LayerNorm weights in the AdamW optimizer.",
            ge=0.0,
            le=1.0,
            title="weight_decay",
        ),
    ] = 0.01
    warmup_ratio: Annotated[
        Optional[float],
        Field(
            description="Ratio of total training steps used for a linear warmup from 0 to the learning rate.",
            ge=0.0,
            title="warmup_ratio",
        ),
    ] = 0.05
    lr_scheduler: Annotated[
        Optional[LRScheduler],
        Field(
            description="The scheduler type to use. See the HuggingFace documentation of `SchedulerType` for all possible values.",
            title="lr_scheduler",
        ),
    ] = "cosine"
    learning_rate: Annotated[
        Optional[float],
        Field(
            description="The initial learning rate for `AdamW` optimizer.",
            gt=0.0,
            lt=1.0,
            title="learning_rate",
        ),
    ] = 0.0005
    lora_r: Annotated[
        Optional[int],
        Field(
            description="The rank of the LoRA update matrices, expressed in int. Lower rank results in smaller update matrices with fewer trainable parameters.",
            gt=0,
            title="lora_r",
        ),
    ] = 32
    lora_alpha_over_r: Annotated[
        Optional[float],
        Field(
            description="The ratio of the LoRA scaling factor (alpha) to the LoRA rank. Empirically, this parameter works well when set to 0.5, 1, or 2.",
            ge=0.5,
            le=3.0,
            title="lora_alpha_over_r",
        ),
    ] = 1
    lora_target_modules: Annotated[
        Optional[List[str]],
        Field(
            description="The list of transformer modules to apply LoRA to. Possible modules: 'q_proj', 'k_proj', 'v_proj', 'o_proj', 'gate_proj', 'up_proj', 'down_proj'",
            title="lora_target_modules",
        ),
    ] = ["q_proj", "k_proj", "v_proj", "o_proj"]
    use_unsloth: Annotated[
        Optional[bool],
        Field(description="Whether to use unsloth.", title="use_unsloth"),
    ] = True
    rope_scaling_factor: Annotated[
        Optional[int],
        Field(
            description="Scale the base LLM's context length by this factor using RoPE scaling. Only works if use_unsloth is set to True.",
            ge=1,
            le=6,
            title="rope_scaling_factor",
        ),
    ] = 1


class MaxSequencesPerExample(str, Enum):
    AUTO = "auto"


class TrainTabularFTConfig(ConfigBase):
    group_training_examples_by: Annotated[
        Optional[Union[str, List[str]]],
        Field(
            description="Column(s) to group training examples by. This is useful when you want the model to learn inter-record correlations for a given grouping of records.",
            title="group_training_examples_by",
        ),
    ] = None
    order_training_examples_by: Annotated[
        Optional[str],
        Field(
            description="Column to order training examples by. This is useful when you want the model to learn sequential relationships for a given ordering of records. If you provide this parameter, you must also provide `group_training_examples_by`.",
            title="order_training_examples_by",
        ),
    ] = None
    max_sequences_per_example: Annotated[
        Optional[Union[int, MaxSequencesPerExample]],
        Field(
            description="If specified, adds at most this number of sequences per example; otherwise, fills up context. Supports 'auto' where a value of 1 is chosen if differential privacy is enabled, and None otherwise. Required for DP to limit contribution of each example.",
            title="max_sequences_per_example",
        ),
    ] = "auto"
    params: Optional[TabularFTTrainingParams] = None
    privacy_params: Optional[TabularFTPrivacyParams] = None


class BinaryEncoderHandler(str, Enum):
    MODE = "mode"


class ConditionalVectorType(str, Enum):
    SINGLE_DISCRETE = "single_discrete"
    ANYWAY = "anyway"


class FilterLevel(str, Enum):
    MEDIUM = "medium"
    HIGH = "high"
    AUTO = "auto"


class GenerateFromTabularGANConfig(ConfigBase):
    num_records: Annotated[
        Optional[int],
        Field(
            description="Number of text outputs to generate.", gt=0, title="num_records"
        ),
    ] = None
    num_records_multiplier: Annotated[
        Optional[float],
        Field(
            description="Calculate the number of text outputs to generate by applying this multiplier to the number of records in the training data. For example, use 1.0 to generate synthetic data of the same size as the training data.",
            gt=0.0,
            title="num_records_multiplier",
        ),
    ] = None
    max_tries: Annotated[
        Optional[int],
        Field(
            description="Max attempts to sample new records, for conditioning/seeding only.",
            ge=1,
            title="Max Tries",
        ),
    ] = 10


class PrivacyFilters(ConfigBase):
    outliers: Optional[FilterLevel] = "medium"
    similarity: Optional[FilterLevel] = "medium"
    max_iterations: Annotated[Optional[int], Field(title="Max Iterations")] = 10


class BatchSize(str, Enum):
    AUTO = "auto"


class Epochs(str, Enum):
    AUTO = "auto"


class ForceConditioning(str, Enum):
    AUTO = "auto"


class TabularGANTrainingParams(ConfigBase):
    embedding_dim: Annotated[Optional[int], Field(title="Embedding Dim")] = 128
    generator_dim: Annotated[Optional[List[int]], Field(title="Generator Dim")] = None
    discriminator_dim: Annotated[
        Optional[List[int]], Field(title="Discriminator Dim")
    ] = None
    generator_lr: Annotated[Optional[float], Field(title="Generator Lr")] = 0.0002
    generator_decay: Annotated[Optional[float], Field(title="Generator Decay")] = 1e-06
    discriminator_lr: Annotated[Optional[float], Field(title="Discriminator Lr")] = (
        0.0002
    )
    discriminator_decay: Annotated[
        Optional[float], Field(title="Discriminator Decay")
    ] = 1e-06
    batch_size: Annotated[
        Optional[Union[int, BatchSize]], Field(title="Batch Size")
    ] = 500
    discriminator_steps: Annotated[
        Optional[int], Field(title="Discriminator Steps")
    ] = 1
    binary_encoder_cutoff: Annotated[
        Optional[int], Field(title="Binary Encoder Cutoff")
    ] = 150
    binary_encoder_nan_handler: Optional[BinaryEncoderHandler] = "mode"
    auto_transform_datetimes: Annotated[
        Optional[bool], Field(title="Auto Transform Datetimes")
    ] = False
    log_frequency: Annotated[Optional[bool], Field(title="Log Frequency")] = True
    cbn_sample_size: Annotated[Optional[int], Field(title="Cbn Sample Size")] = 250000
    epochs: Annotated[Optional[Union[int, Epochs]], Field(title="Epochs")] = 600
    pac: Annotated[Optional[int], Field(title="Pac")] = 10
    data_upsample_limit: Annotated[
        Optional[int], Field(title="Data Upsample Limit")
    ] = 100
    conditional_vector_type: Optional[ConditionalVectorType] = "single_discrete"
    conditional_select_column_prob: Annotated[
        Optional[float], Field(title="Conditional Select Column Prob")
    ] = None
    conditional_select_mean_columns: Annotated[
        Optional[float], Field(title="Conditional Select Mean Columns")
    ] = None
    reconstruction_loss_coef: Annotated[
        Optional[float], Field(title="Reconstruction Loss Coef")
    ] = 1.0
    force_conditioning: Annotated[
        Optional[Union[bool, ForceConditioning]], Field(title="Force Conditioning")
    ] = "auto"


class TrainTabularGANConfig(ConfigBase):
    privacy_filters: Optional[PrivacyFilters] = None
    params: Optional[TabularGANTrainingParams] = None


class TabularGan(ConfigBase):
    train: Optional[TrainTabularGANConfig] = None
    generate: Optional[GenerateFromTabularGANConfig] = None


class TestFailingTask(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100


class TestOptionalArgTask(ConfigBase):
    pass


class TestRequiredAndOptionalArgsTask(ConfigBase):
    pass


class TestTaskCallingTask(ConfigBase):
    pass


class TestUnhandledErrorTask(ConfigBase):
    foo: Annotated[str, Field(title="Foo")]


class GenerateFromTextFTConfig(ConfigBase):
    seed_records_multiplier: Annotated[
        Optional[int],
        Field(
            description="For conditional generation, emit this number of records consecutively for each prompt record in the seed dataset. Defaults to `1`, which means generating exactly one record per seed record. Ignored for unconditional generation.",
            gt=0,
            title="seed_records_multiplier",
        ),
    ] = 1
    maximum_text_length: Annotated[
        Optional[int],
        Field(
            description="Maximum number of tokens to generate (not including the prompt) in output text. Defaults to `42`.",
            gt=0,
            title="maximum_text_length",
        ),
    ] = 42
    top_p: Annotated[
        Optional[float],
        Field(
            description="Defaults to 0.8987. If set to float < 1, only the most probable tokens with probabilities that add up to ``top_p`` or higher are kept for generation.",
            ge=0.0,
            le=1.0,
            title="top_p",
        ),
    ] = 0.8987601335810778
    top_k: Annotated[
        Optional[int],
        Field(
            description="The number of highest probability vocabulary tokens to keep for top-k-filtering. Defaults to `43`.",
            ge=0,
            title="top_k",
        ),
    ] = 43
    num_beams: Annotated[
        Optional[int],
        Field(
            description="Number of beams for beam search. 1 means no beam search. Defaults to `1`.",
            ge=1,
            title="num_beams",
        ),
    ] = 1
    do_sample: Annotated[
        Optional[bool],
        Field(
            description="Whether or not to use sampling; use greedy decoding otherwise. Defaults to `True`.",
            title="do_sample",
        ),
    ] = True
    do_early_stopping: Annotated[
        Optional[bool],
        Field(
            description="Whether to stop the beam search when at least ``num_beams`` sentences are finished per batch or not. Defaults to `True`.",
            title="do_early_stopping",
        ),
    ] = True
    typical_p: Annotated[
        Optional[float],
        Field(
            description="The amount of probability mass from the original distribution that we wish to consider. Defaults to `0.8`.",
            ge=0.0,
            le=1.0,
            title="typical_p",
        ),
    ] = 0.8
    temperature: Annotated[
        Optional[float],
        Field(
            description="Passed as `temperature` argument to the generator. The value used to module the next token probabilities. It defaults to `None`, in which casemodels' default temperature is used or 1.0.",
            gt=0.0,
            title="temperature",
        ),
    ] = None
    use_vllm: Annotated[
        Optional[bool],
        Field(
            description="If True, load the generator using vLLM. Defaults to `False`.",
            title="use_vllm",
        ),
    ] = False
    num_records: Annotated[
        Optional[int],
        Field(
            description="Number of text outputs to generate.", gt=0, title="num_records"
        ),
    ] = None
    num_records_multiplier: Annotated[
        Optional[float],
        Field(
            description="Calculate the number of text outputs to generate by applying this multiplier to the number of records in the training data. For example, use 1.0 to generate synthetic data of the same size as the training data.",
            gt=0.0,
            title="num_records_multiplier",
        ),
    ] = None


class PeftParams(ConfigBase):
    lora_r: Annotated[
        Optional[int],
        Field(
            description="LoRA makes fine-tuning more efficient by drastically reducing the number of trainable parameters by updating weights through two smaller matrices through low-rank decomposition. `lora_r` is the rank of the matrices that are updated. Lower value means fewer trainable parameters.",
            gt=1,
            title="lora_r",
        ),
    ] = 8
    lora_alpha_over_r: Annotated[
        Optional[float],
        Field(
            description="The ratio of the LoRA scaling factor (alpha) to the LoRA rank. Empirically, this value works well when set to 0.5, 1, or 2.",
            ge=0.1,
            le=4.0,
            title="lora_alpha_over_r",
        ),
    ] = 1
    target_modules: Annotated[
        Optional[Union[str, List[str]]],
        Field(
            description="List of module names or regex expression of the module names to replace with LoRA. For example, ['q', 'v'] or '.*decoder.*(SelfAttention|EncDecAttention).*(q|v)$'. This can also be a wildcard 'all-linear' which matches all linear/Conv1D layers except the output layer. If not specified, modules will be chosen according to the model architecture. If the architecture is not known, an error will be raised -- in this case, you should specify the target modules manually.",
            title="Target Modules",
        ),
    ] = None


class Delta2(RootModel[float]):
    root: Annotated[
        float,
        Field(
            description="Probability of accidentally leaking information. Setting to 'auto' usesdelta of 1/n^1.2, where n is the number of training records. The value `1.2` is set in /python/src/gretel_skynet/runtime/model_auto_config/gpt_x.py:L15",
            gt=0.0,
            lt=1.0,
            title="delta",
        ),
    ]


class Delta3(str, Enum):
    AUTO = "auto"


class PrivacyParams(ConfigBase):
    dp: Annotated[
        Optional[bool],
        Field(
            description="Flag to turn on differentially private fine tuning when a data source is provided.",
            title="dp",
        ),
    ] = False
    epsilon: Annotated[
        Optional[float],
        Field(
            description="Target for epsilon when training completes.",
            ge=0.1,
            le=100.0,
            title="epsilon",
        ),
    ] = 8
    delta: Annotated[
        Optional[Union[Delta2, Delta3]],
        Field(
            description="Probability of accidentally leaking information. Setting to 'auto' usesdelta of 1/n^1.2, where n is the number of training records. The value `1.2` is set in /python/src/gretel_skynet/runtime/model_auto_config/gpt_x.py:L15",
            title="delta",
        ),
    ] = "auto"
    per_sample_max_grad_norm: Annotated[
        Optional[float],
        Field(
            description="Maximum L2 norm of per sample gradients.",
            gt=0.0,
            title="per_sample_max_grad_norm",
        ),
    ] = 1.0
    entity_column_name: Annotated[
        Optional[str],
        Field(
            description="Column representing unit of privacy. e.g. `name` or `id`.",
            title="entity_column_name",
        ),
    ] = None
    poisson_sampling: Annotated[
        Optional[bool],
        Field(
            description="Enables Poisson sampling for proper DP accounting",
            title="poisson_sampling",
        ),
    ] = False


class TextFTTrainingParams(ConfigBase):
    batch_size: Annotated[
        Optional[int],
        Field(
            description="The batch size per GPU/TPU core/CPU for training. Defaults to `4`.",
            gt=0,
            title="batch_size",
        ),
    ] = 4
    epochs: Annotated[
        Optional[float],
        Field(
            description="Total number of training epochs to perform on fine-tuning the model. Either this or the steps parameter must be set, but not both.",
            gt=0.0,
            title="epochs",
        ),
    ] = None
    steps: Annotated[
        Optional[int],
        Field(
            description="Total number of training steps to perform on fine-tuning the model. Either this or the epochs parameter must be set, but not both.",
            gt=0,
            title="steps",
        ),
    ] = None
    weight_decay: Annotated[
        Optional[float],
        Field(
            description="The weight decay to apply (if not zero) to all layers except all bias and LayerNorm weights in AdamW optimizer. Defaults to `0.01`.",
            ge=0.0,
            le=1.0,
            title="weight_decay",
        ),
    ] = 0.01
    warmup_steps: Annotated[
        Optional[int],
        Field(
            description="Number of steps used for a linear warmup from `0` to `learning_rate`. Defaults to `100`.",
            gt=0,
            title="warmup_steps",
        ),
    ] = 100
    lr_scheduler: Annotated[
        Optional[LRScheduler],
        Field(
            description="The scheduler type to use. See the HuggingFace documentation of `SchedulerType` for all possible values. Defaults to `linear`.",
            title="lr_scheduler",
        ),
    ] = "linear"
    learning_rate: Annotated[
        Optional[float],
        Field(
            description="The initial learning rate for `AdamW` optimizer. Defaults to `0.0002`.",
            gt=0.0,
            lt=1.0,
            title="learning_rate",
        ),
    ] = 0.0002
    max_tokens: Annotated[
        Optional[int],
        Field(
            description="The maximum length (in number of tokens) for any input record.The tokenizer used corresponds to the pretrained model selected.Defaults to `512`.",
            gt=0,
            title="max_tokens",
        ),
    ] = 512
    gradient_accumulation_steps: Annotated[
        Optional[int],
        Field(
            description="Number of update steps to accumulate the gradients for, before performing a backward/update pass. This technique increases the effective batch size that will fit into GPU memory.",
            gt=0,
            title="gradient_accumulation_steps",
        ),
    ] = 8
    seed: Annotated[
        Optional[int],
        Field(
            description="Random number generator seed passed to HF's Trainer. A null value will result on a random seed being generated.",
            ge=0,
            lt=4294967296,
            title="seed",
        ),
    ] = None


class Validation(RootModel[int]):
    root: Annotated[
        int,
        Field(
            description="Proportion of the training dataset to use for model validation. This value needs to be between 0.0 and 1.0. Defaults to `None`.",
            gt=0,
            title="validation",
        ),
    ]


class Validation1(RootModel[float]):
    root: Annotated[
        float,
        Field(
            description="Proportion of the training dataset to use for model validation. This value needs to be between 0.0 and 1.0. Defaults to `None`.",
            gt=0.0,
            title="validation",
        ),
    ]


class TrainTextFTConfig(ConfigBase):
    pretrained_model: Annotated[
        Optional[str],
        Field(
            description="Select the text generation model to fine-tune from HuggingFace. Defaults to `meta-llama/Llama-3.1-8B-Instruct`.",
            title="pretrained_model",
        ),
    ] = "meta-llama/Llama-3.1-8B-Instruct"
    column_name: Annotated[
        Optional[str],
        Field(
            description="The name of the column to use for training. Defaults to `None`.",
            title="column_name",
        ),
    ] = None
    validation: Annotated[
        Optional[Union[Validation, Validation1]],
        Field(
            description="Proportion of the training dataset to use for model validation. This value needs to be between 0.0 and 1.0. Defaults to `None`.",
            title="validation",
        ),
    ] = None
    params: Optional[TextFTTrainingParams] = None
    peft_params: Optional[PeftParams] = None
    privacy_params: Optional[PrivacyParams] = None


class TextFt(ConfigBase):
    train: Optional[TrainTextFTConfig] = None
    generate: Optional[GenerateFromTextFTConfig] = None


class ClassifyConfig(ConfigBase):
    enable: Annotated[
        Optional[bool],
        Field(description="Enable column classification.", title="Enable"),
    ] = None
    entities: Annotated[
        Optional[List[str]],
        Field(description="List of entity types to classify.", title="Entities"),
    ] = None
    num_samples: Annotated[
        Optional[int],
        Field(
            description="Number of column values to sample for classification.",
            title="Num Samples",
        ),
    ] = 3


class Position(RootModel[int]):
    root: Annotated[int, Field(description="Column position.", ge=0, title="Position")]


class PositionItem(RootModel[int]):
    root: Annotated[int, Field(ge=0)]


class Column(ConfigBase):
    name: Annotated[Optional[str], Field(description="Column name.", title="Name")] = (
        None
    )
    position: Annotated[
        Optional[Union[Position, List[PositionItem]]],
        Field(description="Column position.", title="Position"),
    ] = None
    condition: Annotated[
        Optional[str], Field(description="Column condition.", title="Condition")
    ] = None
    value: Annotated[
        Optional[str], Field(description="Rename to value.", title="Value")
    ] = None
    entity: Annotated[
        Optional[Union[str, List[str]]],
        Field(description="Column entity match.", title="Entity"),
    ] = None
    type: Annotated[
        Optional[Union[str, List[str]]],
        Field(description="Column type match.", title="Type"),
    ] = None


class ColumnActions(ConfigBase):
    add: Annotated[
        Optional[List[Column]], Field(description="Columns to add.", title="Add")
    ] = None
    drop: Annotated[
        Optional[List[Column]], Field(description="Columns to drop.", title="Drop")
    ] = None
    rename: Annotated[
        Optional[List[Column]], Field(description="Columns to rename", title="Rename")
    ] = None


class NERConfig(ConfigBase):
    ner_threshold: Annotated[
        Optional[float],
        Field(description="NER model threshold.", title="Ner Threshold"),
    ] = 0.7
    ner_optimized: Annotated[
        Optional[bool],
        Field(
            description="Whether or not NER runs in an optimized mode (i.e. with a GPU)",
            title="Ner Optimized",
        ),
    ] = True
    enable_regexps: Annotated[
        Optional[bool],
        Field(
            description="Enable NER regular expressions (experimental)",
            title="Enable Regexps",
        ),
    ] = False
    enable_gliner: Annotated[
        Optional[bool],
        Field(description="Enable gliner NER module", title="Enable Gliner"),
    ] = True


class Row(ConfigBase):
    name: Annotated[
        Optional[Union[str, List[str]]], Field(description="Row name.", title="Name")
    ] = None
    condition: Annotated[
        Optional[str], Field(description="Row condition match.", title="Condition")
    ] = None
    foreach: Annotated[
        Optional[str], Field(description="Foreach expression.", title="Foreach")
    ] = None
    value: Annotated[
        Optional[str], Field(description="Row value definition.", title="Value")
    ] = None
    entity: Annotated[
        Optional[Union[str, List[str]]],
        Field(description="Row entity match.", title="Entity"),
    ] = None
    type: Annotated[
        Optional[Union[str, List[str]]],
        Field(description="Row type match.", title="Type"),
    ] = None
    fallback_value: Annotated[
        Optional[str], Field(description="Row fallback value.", title="Fallback Value")
    ] = None


class RowActions(ConfigBase):
    drop: Annotated[
        Optional[List[Row]], Field(description="Rows to drop.", title="Drop")
    ] = None
    update: Annotated[
        Optional[List[Row]], Field(description="Rows to update.", title="Update")
    ] = None


class StepDefinition(ConfigBase):
    vars: Annotated[
        Optional[Dict[str, Union[str, Dict[str, Any], List]]],
        Field(description="Variable names and templates.", title="Vars"),
    ] = None
    columns: Annotated[
        Optional[ColumnActions],
        Field(description="Columns transform configuration.", title="Columns"),
    ] = None
    rows: Annotated[
        Optional[RowActions],
        Field(description="Rows transform configurations.", title="Rows"),
    ] = None


class CodeLang(str, Enum):
    GO = "go"
    JAVASCRIPT = "javascript"
    JAVA = "java"
    KOTLIN = "kotlin"
    PYTHON = "python"
    RUBY = "ruby"
    RUST = "rust"
    SCALA = "scala"
    SWIFT = "swift"
    TYPESCRIPT = "typescript"
    SQL_SQLITE = "sql:sqlite"
    SQL_TSQL = "sql:tsql"
    SQL_BIGQUERY = "sql:bigquery"
    SQL_MYSQL = "sql:mysql"
    SQL_POSTGRES = "sql:postgres"
    SQL_ANSI = "sql:ansi"


class ValidateCode(ConfigBase):
    code_lang: CodeLang
    target_columns: Annotated[List[str], Field(title="Target Columns")]
    result_columns: Annotated[List[str], Field(title="Result Columns")]


class ManualDistribution(ConfigBase):
    distribution_type: Optional[DistributionType] = "manual"
    params: ManualDistributionParams


class UniformDistribution(ConfigBase):
    distribution_type: Optional[DistributionType] = "uniform"
    params: UniformDistributionParams


class ExistingColumn(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    description: Annotated[str, Field(title="Description")]
    output_type: OutputType
    output_format: Annotated[
        Optional[Union[str, Dict[str, Any]]], Field(title="Output Format")
    ] = None


class ExistingColumns(ConfigBase):
    variables: Annotated[Optional[List[ExistingColumn]], Field(title="Variables")] = (
        None
    )


class ColumnConstraintParams(ConfigBase):
    operator: InequalityOperator
    rhs: Annotated[Union[float, str], Field(title="Rhs")]


class ConditionalDataColumn(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    type: SamplerType
    params: Annotated[
        Union[
            SubcategorySamplerParams,
            CategorySamplerParams,
            DatetimeSamplerParams,
            PersonSamplerParams,
            TimeDeltaSamplerParams,
            UUIDSamplerParams,
            BernoulliSamplerParams,
            BernoulliMixtureSamplerParams,
            BinomialSamplerParams,
            GaussianSamplerParams,
            PoissonSamplerParams,
            UniformSamplerParams,
            ScipySamplerParams,
        ],
        Field(title="Params"),
    ]
    conditional_params: Annotated[
        Optional[
            Dict[
                str,
                Union[
                    SubcategorySamplerParams,
                    CategorySamplerParams,
                    DatetimeSamplerParams,
                    PersonSamplerParams,
                    TimeDeltaSamplerParams,
                    UUIDSamplerParams,
                    BernoulliSamplerParams,
                    BernoulliMixtureSamplerParams,
                    BinomialSamplerParams,
                    GaussianSamplerParams,
                    PoissonSamplerParams,
                    UniformSamplerParams,
                    ScipySamplerParams,
                ],
            ]
        ],
        Field(title="Conditional Params"),
    ] = {}
    convert_to: Annotated[Optional[str], Field(title="Convert To")] = None


class SeedCategory(ConfigBase):
    name: Annotated[str, Field(title="Name")]
    description: Annotated[Optional[str], Field(title="Description")] = None
    values: Annotated[Optional[List[Union[str, int, bool]]], Field(title="Values")] = []
    weights: Annotated[Optional[List[float]], Field(title="Weights")] = []
    num_new_values_to_generate: Annotated[
        Optional[NumNewValuesToGenerate], Field(title="Num New Values To Generate")
    ] = None
    subcategories: Annotated[
        Optional[List[SeedSubcategory]], Field(max_length=5, title="Subcategories")
    ] = []
    quality_rank: Annotated[Optional[int], Field(title="Quality Rank")] = None
    generated_values: Annotated[
        Optional[List[Union[str, int, bool]]], Field(title="Generated Values")
    ] = []


class LoadDataSeeds(ConfigBase):
    seed_categories: Annotated[List[SeedCategory], Field(title="Seed Categories")]
    dataset_schema_map: Annotated[
        Optional[Dict[str, Any]], Field(title="Dataset Schema Map")
    ] = None


class GenerateFromTabularFTConfig(ConfigBase):
    num_records: Annotated[
        Optional[int],
        Field(
            description="Number of records to generate. If you want to generate more than 130000 records, please break the generation job into smaller batches, which you can run in parallel.",
            ge=0,
            title="num_records",
        ),
    ] = 5000
    temperature: Annotated[
        Optional[float],
        Field(
            description="The value used to control the randomness of the generated data. Higher values make the data more random.",
            gt=0.0,
            title="temperature",
        ),
    ] = 0.75
    repetition_penalty: Annotated[
        Optional[float],
        Field(
            description="The value used to control the likelihood of the model repeating the same token.",
            gt=0.0,
            title="repetition_penalty",
        ),
    ] = 1.2
    top_p: Annotated[
        Optional[float],
        Field(
            description="The cumulative probability cutoff for sampling tokens.",
            gt=0.0,
            le=1.0,
            title="top_p",
        ),
    ] = 1.0
    stop_params: Annotated[
        Optional[TabularFTGenerateStopParams],
        Field(
            description="Optional mechanism to stop generation if too many invalid records are being created. This helps guard against extremely long generation jobs that likely do not have the potential to generate high-quality data.",
            title="Stop Params",
        ),
    ] = None
    use_structured_generation: Annotated[
        Optional[bool],
        Field(
            description="Whether to perform structure generation using Outlines.",
            title="use_structured_generation",
        ),
    ] = False


class TabularFt(ConfigBase):
    train: Optional[TrainTabularFTConfig] = None
    generate: Optional[GenerateFromTabularFTConfig] = None


class Globals(ConfigBase):
    locales: Annotated[
        Optional[List[str]],
        Field(description="list of locales.", examples=["en_US"], title="Locales"),
    ] = None
    seed: Annotated[
        Optional[int],
        Field(
            description="Optional random seed.",
            gt=-2147483647,
            lt=2147483647,
            title="Seed",
        ),
    ] = None
    classify: Annotated[
        Optional[ClassifyConfig],
        Field(description="Column classification configuration", title="Classify"),
    ] = {"enable": None, "entities": None, "num_samples": 3}
    ner: Annotated[
        Optional[NERConfig],
        Field(description="Named Entity Recognition configuration", title="Ner"),
    ] = {
        "ner_threshold": 0.7,
        "ner_optimized": True,
        "enable_regexps": False,
        "enable_gliner": True,
    }
    lock_columns: Annotated[
        Optional[List[str]],
        Field(
            description="List of columns to preserve as immutable across all transformations.",
            title="Lock Columns",
        ),
    ] = None


class Transform(ConfigBase):
    globals: Annotated[
        Optional[Globals], Field(description="Global config options.", title="Globals")
    ] = {
        "locales": None,
        "seed": None,
        "classify": {"enable": None, "entities": None, "num_samples": 3},
        "ner": {
            "ner_threshold": 0.7,
            "ner_optimized": True,
            "enable_regexps": False,
            "enable_gliner": True,
        },
        "lock_columns": None,
    }
    steps: Annotated[
        List[StepDefinition],
        Field(
            description="list of transform steps to perform on input.",
            max_length=10,
            min_length=1,
            title="Steps",
        ),
    ]
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2


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


class EvaluateDataDesignerDataset(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "text"
    llm_judge_column: Annotated[Optional[str], Field(title="Llm Judge Column")] = ""
    columns_to_ignore: Annotated[
        Optional[List[str]], Field(title="Columns To Ignore")
    ] = None
    validation_columns: Annotated[
        Optional[List[str]], Field(title="Validation Columns")
    ] = None


class GenerateColumnFromTemplateV2Config(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "text"
    prompt: Annotated[str, Field(title="Prompt")]
    name: Annotated[Optional[str], Field(title="Name")] = "response"
    system_prompt: Annotated[Optional[str], Field(title="System Prompt")] = None
    output_type: Optional[OutputType] = "text"
    output_format: Annotated[
        Optional[Union[str, Dict[str, Any]]], Field(title="Output Format")
    ] = None
    description: Annotated[Optional[str], Field(title="Description")] = ""


class GenerateColumnConfigFromInstruction(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "code"
    name: Annotated[str, Field(title="Name")]
    instruction: Annotated[str, Field(title="Instruction")]
    edit_task: Optional[GenerateColumnFromTemplateV2Config] = None
    existing_columns: Annotated[Optional[ExistingColumns], Field()] = {"variables": []}
    use_reasoning: Annotated[Optional[bool], Field(title="Use Reasoning")] = True
    must_depend_on: Annotated[Optional[List[str]], Field(title="Must Depend On")] = None


class GenerateColumnFromTemplate(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "text"
    prompt: Annotated[str, Field(title="Prompt")]
    name: Annotated[Optional[str], Field(title="Name")] = "response"
    system_prompt: Annotated[Optional[str], Field(title="System Prompt")] = None
    data_config: DataConfig
    description: Annotated[Optional[str], Field(title="Description")] = ""


class GenerateColumnFromTemplateV2(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "text"
    prompt: Annotated[str, Field(title="Prompt")]
    name: Annotated[Optional[str], Field(title="Name")] = "response"
    system_prompt: Annotated[Optional[str], Field(title="System Prompt")] = None
    output_type: Optional[OutputType] = "text"
    output_format: Annotated[
        Optional[Union[str, Dict[str, Any]]], Field(title="Output Format")
    ] = None
    description: Annotated[Optional[str], Field(title="Description")] = ""


class ColumnConstraint(ConfigBase):
    target_column: Annotated[str, Field(title="Target Column")]
    type: ConstraintType
    params: ColumnConstraintParams


class DataSchema(ConfigBase):
    columns: Annotated[
        List[ConditionalDataColumn], Field(min_length=1, title="Columns")
    ]
    constraints: Annotated[
        Optional[List[ColumnConstraint]], Field(title="Constraints")
    ] = []


class GenerateColumnsUsingSamplers(ConfigBase):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100
    data_schema: DataSchema
    max_rejections_factor: Annotated[
        Optional[int], Field(title="Max Rejections Factor")
    ] = 5


class GenerateSamplingColumnConfigFromInstruction(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "code"
    name: Annotated[str, Field(title="Name")]
    instruction: Annotated[str, Field(title="Instruction")]
    edit_task: Optional[DataColumnFromSamplingConfig] = None


class GenerateSeedCategoryValues(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "text"
    seed_categories: Annotated[List[SeedCategory], Field(title="Seed Categories")]
    dataset_context: Annotated[Optional[str], Field(title="Dataset Context")] = ""


class JudgeWithLlm(ConfigBase):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"
    error_rate: Annotated[
        Optional[float], Field(ge=0.0, le=1.0, title="Error Rate")
    ] = 0.2
    model_configs: Annotated[
        Optional[List[ModelConfig]], Field(title="Model Configs")
    ] = None
    model_alias: Annotated[
        Optional[Union[str, ModelAlias]], Field(title="Model Alias")
    ] = "judge"
    prompt: Annotated[
        str,
        Field(
            description="Template for generating prompts. Use Jinja2 templates to reference dataset columns.",
            title="Prompt",
        ),
    ]
    num_samples_to_judge: Annotated[
        Optional[int],
        Field(
            description="Number of samples to judge. Default is 100.",
            title="Num Samples To Judge",
        ),
    ] = 100
    rubrics: Annotated[
        List[Rubric],
        Field(
            description="List of rubric configurations to use for evaluation. At least one must be provided.",
            min_length=1,
            title="Rubrics",
        ),
    ]
    result_column: Annotated[
        Optional[str],
        Field(description="Column name to store judge results.", title="Result Column"),
    ] = "llm_judge_results"
