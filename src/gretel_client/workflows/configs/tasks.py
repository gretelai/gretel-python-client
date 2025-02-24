# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, RootModel


class Combiner(BaseModel):
    pass


class DummyTaskWithInputs(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5


class DummyTaskWithListOfInputs(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5


class EvaluateDataset(BaseModel):
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


class SystemPromptType(str, Enum):
    reflection = "reflection"
    cognition = "cognition"


class ExtractDataSeedsFromSampleRecords(BaseModel):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
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


class LLMType(str, Enum):
    natural_language = "natural_language"
    code = "code"
    judge = "judge"


class OutputType(str, Enum):
    code = "code"
    text = "text"
    structured = "structured"


class ConstraintType(str, Enum):
    scalar_inequality = "scalar_inequality"
    column_inequality = "column_inequality"


class DataSourceParams(BaseModel):
    pass


class SerializableConstraint(BaseModel):
    column_name: Annotated[str, Field(title="Column Name")]
    constraint_type: ConstraintType
    params: Annotated[Dict[str, Union[float, str]], Field(title="Params")]


class SourceType(str, Enum):
    bernoulli = "bernoulli"
    binomial = "binomial"
    category = "category"
    datetime = "datetime"
    expression = "expression"
    gaussian = "gaussian"
    poisson = "poisson"
    scipy = "scipy"
    timedelta = "timedelta"
    uniform = "uniform"
    uuid = "uuid"


class GenerateDatasetFromSampleRecords(BaseModel):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
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


class NumNewValuesToGenerate(RootModel[int]):
    root: Annotated[int, Field(gt=0, le=25, title="Num New Values To Generate")]


class SeedSubcategory(BaseModel):
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


class GetGretelDataset(BaseModel):
    name: Annotated[str, Field(title="Name")]


class IdGenerator(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5


class LLMJudgePromptTemplateType(str, Enum):
    text_to_python = "text_to_python"
    text_to_sql = "text_to_sql"


class JudgeWithLlm(BaseModel):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
    judge_template_type: LLMJudgePromptTemplateType
    instruction_column_name: Annotated[str, Field(title="Instruction Column Name")]
    response_column_name: Annotated[str, Field(title="Response Column Name")]
    context_column_name: Annotated[
        Optional[str], Field(title="Context Column Name")
    ] = None
    num_samples_to_judge: Annotated[
        Optional[int], Field(title="Num Samples To Judge")
    ] = 100


class NameGenerator(BaseModel):
    column_name: Annotated[Optional[str], Field(title="Column Name")] = "name"
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5
    seed: Annotated[Optional[int], Field(title="Seed")] = None
    should_fail: Annotated[Optional[bool], Field(title="Should Fail")] = False


class SampleDataSeeds(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 10


class SeedFromRecords(BaseModel):
    records: Annotated[List[Dict[str, Any]], Field(title="Records")]


class LRScheduler(str, Enum):
    cosine = "cosine"
    linear = "linear"
    cosine_with_restarts = "cosine_with_restarts"
    polynomial = "polynomial"
    constant = "constant"
    constant_with_warmup = "constant_with_warmup"


class NavigatorFTGenerateStopParams(BaseModel):
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
    ] = 0.9


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
    auto = "auto"


class NavigatorFTPrivacyParams(BaseModel):
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
    auto = "auto"


class NavigatorFTTrainingParams(BaseModel):
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
    auto = "auto"


class TrainTabularFTConfig(BaseModel):
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
    params: Optional[NavigatorFTTrainingParams] = None
    privacy_params: Optional[NavigatorFTPrivacyParams] = None


class BatchSize(str, Enum):
    auto = "auto"


class Epochs(str, Enum):
    auto = "auto"


class ForceConditioning(str, Enum):
    auto = "auto"


class BinaryEncoderHandler(str, Enum):
    mode = "mode"


class ConditionalVectorType(str, Enum):
    single_discrete = "single_discrete"
    anyway = "anyway"


class FilterLevel(str, Enum):
    medium = "medium"
    high = "high"
    auto = "auto"


class GenerateFromTabularGANConfig(BaseModel):
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


class PrivacyFilters(BaseModel):
    outliers: Optional[FilterLevel] = "medium"
    similarity: Optional[FilterLevel] = "medium"
    max_iterations: Annotated[Optional[int], Field(title="Max Iterations")] = 10


class TestFailingTask(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5


class TestOptionalArgTask(BaseModel):
    pass


class TestRequiredAndOptionalArgsTask(BaseModel):
    pass


class TestTaskCallingTask(BaseModel):
    pass


class ClassifyConfig(BaseModel):
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


class Column(BaseModel):
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


class ColumnActions(BaseModel):
    add: Annotated[
        Optional[List[Column]], Field(description="Columns to add.", title="Add")
    ] = None
    drop: Annotated[
        Optional[List[Column]], Field(description="Columns to drop.", title="Drop")
    ] = None
    rename: Annotated[
        Optional[List[Column]], Field(description="Columns to rename", title="Rename")
    ] = None


class NERConfig(BaseModel):
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


class Row(BaseModel):
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


class RowActions(BaseModel):
    drop: Annotated[
        Optional[List[Row]], Field(description="Rows to drop.", title="Drop")
    ] = None
    update: Annotated[
        Optional[List[Row]], Field(description="Rows to update.", title="Update")
    ] = None


class StepDefinition(BaseModel):
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
    python = "python"
    sqlite = "sqlite"
    tsql = "tsql"
    bigquery = "bigquery"
    mysql = "mysql"
    postgres = "postgres"
    ansi = "ansi"


class ValidateCode(BaseModel):
    code_lang: CodeLang
    code_columns: Annotated[Optional[List[str]], Field(title="Code Columns")] = ["code"]


class DataConfig(BaseModel):
    type: Optional[OutputType] = "text"
    params: Annotated[Optional[Dict[str, Any]], Field(title="Params")] = None


class GenerateColumnFromTemplate(BaseModel):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
    prompt_template: Annotated[str, Field(title="Prompt Template")]
    response_column_name: Annotated[
        Optional[str], Field(title="Response Column Name")
    ] = "response"
    system_prompt: Annotated[Optional[str], Field(title="System Prompt")] = None
    llm_type: Optional[LLMType] = "natural_language"
    data_config: DataConfig


class ConditionalDataColumn(BaseModel):
    name: Annotated[str, Field(title="Name")]
    conditional_params: Annotated[
        Optional[Dict[str, Union[Dict[str, Any], DataSourceParams]]],
        Field(title="Conditional Params"),
    ] = {}
    convert_to: Annotated[Optional[str], Field(title="Convert To")] = None
    source_type: SourceType
    params: Annotated[Union[Dict[str, Any], DataSourceParams], Field(title="Params")]


class ConditionalDataColumnFromDefinition(BaseModel):
    name: Annotated[str, Field(title="Name")]
    conditional_params: Annotated[
        Optional[Dict[str, Union[Dict[str, Any], DataSourceParams]]],
        Field(title="Conditional Params"),
    ] = {}
    convert_to: Annotated[Optional[str], Field(title="Convert To")] = None
    definition: Annotated[str, Field(title="Definition")]


class DataSchema(BaseModel):
    columns: Annotated[
        List[Union[ConditionalDataColumn, ConditionalDataColumnFromDefinition]],
        Field(min_length=1, title="Columns"),
    ]
    constraints: Annotated[
        Optional[List[SerializableConstraint]], Field(title="Constraints")
    ] = []


class GenerateColumnsUsingSamplers(BaseModel):
    data_schema: DataSchema
    num_records: Annotated[int, Field(title="Num Records")]
    max_rejections_factor: Annotated[
        Optional[int], Field(title="Max Rejections Factor")
    ] = 5


class SeedCategory(BaseModel):
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


class GenerateSeedCategoryValues(BaseModel):
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = None
    seed_categories: Annotated[List[SeedCategory], Field(title="Seed Categories")]
    dataset_context: Annotated[Optional[str], Field(title="Dataset Context")] = ""


class LoadDataSeeds(BaseModel):
    seed_categories: Annotated[List[SeedCategory], Field(title="Seed Categories")]
    dataset_schema_map: Annotated[
        Optional[Dict[str, Any]], Field(title="Dataset Schema Map")
    ] = None


class NavigatorFTGenerateParams(BaseModel):
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
        Optional[NavigatorFTGenerateStopParams],
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


class ActganModelHyperparams(BaseModel):
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


class TrainTabularGANConfig(BaseModel):
    privacy_filters: Optional[PrivacyFilters] = None
    params: Optional[ActganModelHyperparams] = None


class TabularGan(BaseModel):
    train: TrainTabularGANConfig
    generate: GenerateFromTabularGANConfig


class Globals(BaseModel):
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


class Transform(BaseModel):
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


class GenerateFromTabularFTConfig(BaseModel):
    params: Optional[NavigatorFTGenerateParams] = None


class TabularFt(BaseModel):
    train: TrainTabularFTConfig
    generate: GenerateFromTabularFTConfig
