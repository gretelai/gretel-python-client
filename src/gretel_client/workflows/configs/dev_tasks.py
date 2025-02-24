# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from __future__ import annotations

from enum import Enum
from typing import Annotated, Dict, List, Optional, Union

from pydantic import BaseModel, Field, RootModel


class AzureDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    container: Annotated[str, Field(title="Container")]


class AzureSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    container: Annotated[str, Field(title="Container")]


class BatchDummyTask(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 5


class BigqueryDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class Query(BaseModel):
    name: Annotated[str, Field(title="Name")]
    query: Annotated[str, Field(title="Query")]


class BigquerySource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class DatabricksDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class DatabricksSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class GcsDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class GcsSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


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


class GenerateFromTabularGan(BaseModel):
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


class GeneratePersons(BaseModel):
    num_records: Annotated[int, Field(title="Num Records")]
    column_name: Annotated[Optional[str], Field(title="Column Name")] = "person"
    allowed_values: Annotated[
        Optional[Dict[str, List[str]]], Field(title="Allowed Values")
    ] = None
    seed: Annotated[Optional[int], Field(title="Seed")] = None


class InspectManagedAsset(BaseModel):
    blob: Annotated[str, Field(title="Blob")]


class MssqlDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class MssqlSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class MysqlDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class MysqlSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class OracleDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class OracleSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class PostgresDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class PostgresSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class QueryManagedDataset(BaseModel):
    query: Annotated[str, Field(title="Query")]


class RunDiagnostics(BaseModel):
    blob: Annotated[str, Field(title="Blob")]
    model_suite: Annotated[str, Field(title="Model Suite")]


class S3Destination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class S3Source(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    path: Annotated[str, Field(title="Path")]
    bucket: Annotated[str, Field(title="Bucket")]


class SnowflakeDestination(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    table: Annotated[str, Field(title="Table")]


class SnowflakeSource(BaseModel):
    connection_id: Annotated[str, Field(title="Connection Id")]
    queries: Annotated[List[Query], Field(max_length=1, min_length=1, title="Queries")]


class LRScheduler(str, Enum):
    cosine = "cosine"
    linear = "linear"
    cosine_with_restarts = "cosine_with_restarts"
    polynomial = "polynomial"
    constant = "constant"
    constant_with_warmup = "constant_with_warmup"


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


class TrainTabularFt(BaseModel):
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


class PrivacyFilters(BaseModel):
    outliers: Optional[FilterLevel] = "medium"
    similarity: Optional[FilterLevel] = "medium"
    max_iterations: Annotated[Optional[int], Field(title="Max Iterations")] = 10


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


class GenerateFromTabularFt(BaseModel):
    params: Optional[NavigatorFTGenerateParams] = None


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


class TrainTabularGan(BaseModel):
    privacy_filters: Optional[PrivacyFilters] = None
    params: Optional[ActganModelHyperparams] = None
