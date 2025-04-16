import json
import logging

from pathlib import Path
from typing import Type

import pandas as pd

from pydantic import BaseModel
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer
from typing_extensions import Self

from gretel_client.data_designer.aidd_config import AIDDConfig
from gretel_client.data_designer.constants import (
    DEFAULT_REPR_HTML_STYLE,
    MODEL_DUMP_KWARGS,
    NUM_PREVIEW_RECORDS,
    REPR_HTML_TEMPLATE,
    REPR_LIST_LENGTH_USE_JSON,
)
from gretel_client.data_designer.dag import topologically_sort_columns
from gretel_client.data_designer.log import get_logger
from gretel_client.data_designer.magic_data_designer import MagicDataDesignerEditor
from gretel_client.data_designer.preview import PreviewResults
from gretel_client.data_designer.types import (
    AIDDColumnT,
    CodeValidationColumn,
    ColumnProviderTypeT,
    DAGColumnT,
    DataSeedColumn,
    EvaluateDataDesignerDatasetSettings,
    EvaluationReportT,
    ExpressionColumn,
    GeneralDatasetEvaluation,
    LLMCodeColumn,
    LLMGenColumn,
    LLMJudgeColumn,
    LLMStructuredColumn,
    LLMTextColumn,
    ModelSuite,
    ProviderType,
    SamplerColumn,
    SeedDataset,
)
from gretel_client.data_designer.utils import (
    CallbackOnMutateDict,
    camel_to_kebab,
    fetch_config_if_remote,
    get_sampler_params,
    get_task_log_emoji,
    make_date_obj_serializable,
    smart_load_dataframe,
)
from gretel_client.data_designer.validate import (
    rich_print_violations,
    validate_aidd_columns,
    Violation,
    ViolationLevel,
)
from gretel_client.data_designer.viz_tools import AIDDMetadata
from gretel_client.files.interface import File
from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.workflows.builder import (
    Message,
    WorkflowBuilder,
    WorkflowInterruption,
    WorkflowValidationError,
)
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.tasks import (
    CodeLang,
    ColumnConstraint,
    ColumnConstraintParams,
    ConstraintType,
    DataSchema,
)
from gretel_client.workflows.configs.tasks import Dtype as ExprDtype
from gretel_client.workflows.configs.tasks import (
    GenerateColumnsUsingSamplers,
    OutputType,
    PersonSamplerParams,
    SamplerType,
    SamplingStrategy,
)
from gretel_client.workflows.configs.workflows import Globals, ModelConfig
from gretel_client.workflows.manager import WorkflowManager
from gretel_client.workflows.tasks import TaskConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = get_logger(__name__, level=logging.INFO)


_type_builtin = type
_SAMPLER_PARAMS: dict[SamplerType, Type[BaseModel]] = get_sampler_params()


class DataDesignerValidationError(Exception): ...


def handle_workflow_validation_error(func):

    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except WorkflowValidationError as e:
            err_message = (
                "🛑 Validation error(s) found! Please correct the following and retry:"
            )
            violations = ""
            for violation in e.field_violations:
                violations += f"\n|-- {violation.error_message}"
                if camel_to_kebab(GenerateColumnsUsingSamplers.__name__) == e.task_name:
                    violations += f"\n|  |-- Field path: {violation.field}"
            err_message += violations
            raise DataDesignerValidationError(err_message) from None

    return wrapper


class DataDesigner:
    """High-level interface for building datasets with AI Data Designer.

    Instances of this class should always be created using the `DataDesignerFactory`
    as an attribute of the Gretel object.

    Example::

        from gretel_client import Gretel

        gretel = Gretel(api_key="prompt")

        # Create a DataDesigner instance from scratch
        aidd = gretel.data_designer.new()

        # Create a DataDesigner instance from a configuration file
        aidd = gretel.data_designer.from_config("path/to/config.yaml")
    """

    @classmethod
    def from_config(
        cls,
        gretel_resource_provider: GretelResourceProviderProtocol,
        config: dict | str | Path,
    ) -> Self:
        json_config = make_date_obj_serializable(
            smart_load_yaml(fetch_config_if_remote(config))
        )
        valid_config: AIDDConfig = AIDDConfig.model_validate(json_config)
        columns = {}
        for col in valid_config.columns:
            if isinstance(col, LLMGenColumn):
                col_dict = col.model_dump()
                match col.output_type:
                    case OutputType.STRUCTURED:
                        col = LLMStructuredColumn(**col_dict)
                    case OutputType.CODE:
                        col = LLMCodeColumn(**col_dict)
                    case _:
                        col = LLMTextColumn(**col_dict)
            columns[col.name] = col
        constraints = (
            {c.target_column: c for c in valid_config.constraints}
            if len(valid_config.constraints or []) > 0
            else {}
        )
        return cls(
            gretel_resource_provider=gretel_resource_provider,
            model_suite=valid_config.model_suite,
            model_configs=valid_config.model_configs,
            seed_dataset=valid_config.seed_dataset,
            person_samplers=valid_config.person_samplers,
            columns=columns,
            constraints=constraints,
            evaluation_report=valid_config.evaluation_report,
        )

    def __init__(
        self,
        *,
        gretel_resource_provider: GretelResourceProviderProtocol,
        model_suite: ModelSuite = ModelSuite.APACHE_2_0,
        model_configs: list[ModelConfig] | None = None,
        seed_dataset: SeedDataset | None = None,
        person_samplers: dict[str, PersonSamplerParams] | None = None,
        columns: dict[str, AIDDColumnT] | None = None,
        constraints: dict[str, ColumnConstraint] | None = None,
        evaluation_report: EvaluationReportT | None = None,
    ):
        self._gretel_resource_provider = gretel_resource_provider
        self._model_suite = model_suite
        self._model_configs = model_configs
        self._seed_dataset = seed_dataset
        self._evaluation_report = evaluation_report
        self._task_registry = Registry()
        self._files = self._gretel_resource_provider.files
        self._workflow_manager = self._gretel_resource_provider.workflows
        self._repr_html_style = DEFAULT_REPR_HTML_STYLE
        self._latent_person_columns: dict[str, PersonSamplerParams] = {}
        self._constraints = constraints or {}

        ## Synchronization: Cause any mutation of these dictionaries to trigger a reset on the magic object
        self.magic = MagicDataDesignerEditor(self)
        self._columns = CallbackOnMutateDict(self.magic.reset)
        self._columns |= columns or {}

        if seed_dataset:
            self.with_seed_dataset(
                seed_dataset.file_id,
                sampling_strategy=seed_dataset.sampling_strategy,
                with_replacement=seed_dataset.with_replacement,
            )

        if person_samplers:
            self.with_person_samplers(person_samplers)

    @property
    def allowed_references(self) -> list[str]:
        """All referenceable variables allowed in prompt templates and expressions."""
        seed_column_names = [c.name for c in self.seed_columns]
        sampler_column_names = [c.name for c in self.sampler_columns]
        dag_column_names = sum(
            [[c.name] + c.side_effect_columns for c in self._dag_columns], []
        )
        return seed_column_names + sampler_column_names + dag_column_names

    @property
    def config(self) -> AIDDConfig:
        """The current configuration object of this Data Designer instance."""
        columns = [
            c
            for c in self._columns.values()
            if c.name not in self._latent_person_columns
            if not isinstance(c, DataSeedColumn)
        ]
        person_samplers = {
            c.name: c.params
            for c in self.sampler_columns
            if c.type == SamplerType.PERSON
            if c.name in self._latent_person_columns
        }
        return AIDDConfig(
            model_suite=self.model_suite,
            model_configs=self.model_configs,
            seed_dataset=self._seed_dataset,
            person_samplers=person_samplers or None,
            columns=columns,
            constraints=list(self._constraints.values()),
            evaluation_report=self._evaluation_report,
        )

    @property
    def model_suite(self) -> ModelSuite:
        """The current model suite."""
        return self._model_suite

    @property
    def model_configs(self) -> list[ModelConfig] | None:
        """The current model configurations."""
        return self._model_configs

    def get_column(self, column_name: str) -> AIDDColumnT | None:
        """Returns the column object with the given name."""
        return self._columns.get(column_name, None)

    def get_columns_of_type(self, column_type: Type) -> list[AIDDColumnT]:
        """Returns all columns of the given type."""
        return [col for col in self._columns.values() if isinstance(col, column_type)]

    def delete_column(self, column_name: str) -> Self:
        """Deletes the column with the given name."""
        if isinstance(self._columns.get(column_name), DataSeedColumn):
            raise ValueError(
                "Seed columns cannot be deleted. Please update the seed dataset instead."
            )
        self._columns.pop(column_name, None)
        return self

    def add_column(
        self,
        column: AIDDColumnT | None = None,
        *,
        name: str | None = None,
        type: ColumnProviderTypeT = ProviderType.LLM_TEXT,
        **kwargs,
    ) -> Self:
        """Add AIDD column to the current Data Designer configuration.

        If no column object is provided, you must provide the `name`, `type`, and any
        additional keyword arguments that are required by the column constructor. For
        each column type, you can directly access their constructor parameters by
        importing from the `params` module: `gretel_client.data_designer.params`.

        Args:
            column: AIDD column object to add.
            name: Name of the column to add. This is only used if `column` is not provided.
            type: Column type to add. This is only used if `column` is not provided.
            **kwargs: Additional keyword arguments to pass to the column constructor.

        Returns:
            The current Data Designer instance.
        """
        if column is None:
            if isinstance(type, str):
                type = _validate_column_provider_type(type)
            column = get_column_from_kwargs(name=name, type=type, **kwargs)
        if not isinstance(column, AIDDColumnT):
            raise ValueError(
                f"🛑 {_type_builtin(column)} is not a valid column type. "
                f"Columns must be one of {[t.__name__ for t in AIDDColumnT.__args__]}."
            )
        if column.name in self._latent_person_columns:
            if (
                not isinstance(column, SamplerColumn)
                or column.type != SamplerType.PERSON
            ):
                raise ValueError(
                    f"🛑 The name `{column.name}` is already the name of a person sampler created "
                    "using `with_person_samplers`. Please ensure that all person sampler and "
                    "column names are unique."
                )
            self._latent_person_columns[column.name] = column.params
        self._columns[column.name] = column
        return self

    def get_constraint(self, target_column: str) -> ColumnConstraint | None:
        """Returns the constraint for the given target column."""
        return self._constraints.get(target_column, None)

    def delete_constraint(self, target_column: str) -> Self:
        """Deletes the constraint for the given target column."""
        self._constraints.pop(target_column, None)
        return self

    def add_constraint(
        self,
        target_column: str,
        type: ConstraintType,
        params: dict[str, str | float] | ColumnConstraintParams,
    ) -> Self:
        """Add a constraint to the current Data Designer configuration.

        Currently, constraints are only supported for numerical samplers.
        The `type` must be one of:
            - "scalar_inequality": Constraint between a column and a scalar value.
            - "column_inequality": Constraint between two columns.

        The `params` must be a dictionary of `ColumnConstraintParams` object with the
        following keyword arguments:
            - "rhs": The right-hand side of the inequality.
            - "operator": The operator for the inequality. It must be one of:
                - "gt": Greater than.
                - "ge": Greater than or equal to.
                - "lt": Less than.
                - "le": Less than or equal to.

        Args:
            target_column: The column that the constraint applies to.
            type: Type of constraint to add.
            params: Parameters for the constraint.

        Returns:
            The current Data Designer instance.
        """
        if isinstance(params, dict):
            params = ColumnConstraintParams.model_validate(params)
        self._constraints[target_column] = ColumnConstraint(
            target_column=target_column,
            type=type,
            params=params,
        )
        return self

    def get_evaluation_report(self) -> EvaluationReportT | None:
        """Returns the dataset evaluation report configuration if one is set."""
        return self._evaluation_report

    def delete_evaluation_report(self) -> Self:
        """Deletes the current dataset evaluation report configuration."""
        self._evaluation_report = None
        return self

    def with_evaluation_report(
        self, settings: EvaluateDataDesignerDatasetSettings | None = None
    ) -> Self:
        """Add an evaluation report to the current Data Designer configuration.

        Args:
            settings: Evaluation report settings.

        Returns:
            The current Data Designer instance.
        """
        self._evaluation_report = GeneralDatasetEvaluation(
            settings=settings
            or EvaluateDataDesignerDatasetSettings(
                llm_judge_column=(
                    ""
                    if len(self.llm_judge_columns) == 0
                    else self.llm_judge_columns[0].name
                ),
                validation_columns=[c.name for c in self.code_validation_columns],
                defined_categorical_columns=[c.name for c in self._categorical_columns],
            )
        )
        return self

    def preview(
        self, verbose_logging: bool = False, validate: bool = True
    ) -> PreviewResults:
        """Generate a preview of the dataset.

        The preview consists of 10 records generated from the current configuration.
        This is a quick way to check that the configuration is working as expected
        before generating a larger dataset.

        Args:
            verbose_logging: Whether to enable verbose logging.
            validate: If True, run semantic validation on the configuration before
                generating a preview. This is recommended to catch issues like invalid
                references in prompt templates, which otherwise would only be caught
                during at runtime.

        Returns:
            Preview results object.
        """
        if validate:
            self._run_semantic_validation(raise_exceptions=True)
        logger.info("🚀 Generating preview")
        workflow = self._build_workflow(verbose_logging=verbose_logging, streaming=True)

        preview = self._capture_preview_result(
            workflow, verbose_logging=verbose_logging
        )
        if preview.dataset is not None and preview.success:
            logger.info("🎉 Your dataset preview is ready!")
        else:
            logger.error(
                "🛑 Something has gone wrong during preview generation. Please inspect "
                "the generated data and adjust your configuration as needed."
            )

        # TODO: Re-visit how we display evaluation results in light of generalized judge-with-llm
        # if preview.evaluation_results is not None:
        #     display_preview_evaluation_summary(
        #         settings.judge_template_type, preview.evaluation_results
        #     )
        return preview

    def create(
        self,
        *,
        num_records: int,
        name: str | None = None,
        wait_until_done: bool = False,
    ) -> WorkflowRun:
        """Create a new dataset based on the current Data Designer configuration.

        This method creates a persistent workflow and runs it as a batch job in a
        managed service. Unlike preview, this creates a permanent record of the
        workflow execution that can be referenced later.

        Args:
            num_records: Number of records to generate.
            name: Name of the workflow.
            wait_until_done: Block until the workflow has completed running.
                If False, immediately returns the WorkflowRun object.

        Returns:
            WorkflowRun object.
        """
        logger.info("🚀 Submitting batch workflow")
        workflow = self._build_workflow(num_records=num_records)
        return workflow.run(name=name, wait_until_done=wait_until_done)

    def with_person_samplers(
        self,
        person_samplers: dict[str, PersonSamplerParams],
        *,
        keep_person_columns: bool = False,
    ) -> Self:
        """Define latent person samplers that will be dropped at the end of the workflow.

        Person samplers defined with this method are latent in the sense that they give
        you access to person objects with attributes that can be referenced by other columns,
        but the objects themselves are dropped from the final dataset. This is useful
        when you just need access to certain person attributes but don't need the entire
        object in the final dataset.

        If you want to keep the person sampler columns in the final dataset, you have two
        options. You can either set `keep_person_columns` to `True` or you can add person
        samplers as columns using the `add_column` method.

        Args:
            person_samplers: Dictionary of person sampler parameters. The keys are the names
                of the person samplers and the values are the parameters for each sampler.
            keep_person_columns: If True, keep the person sampler columns in the final dataset.

        Returns:
            The current Data Designer instance.
        """
        for name, params in person_samplers.items():
            person_params = PersonSamplerParams.model_validate(params)
            self.add_column(
                SamplerColumn(
                    name=name,
                    type=SamplerType.PERSON,
                    params=person_params.model_dump(),
                )
            )
            if not keep_person_columns:
                self._latent_person_columns[name] = person_params

        return self

    def with_seed_dataset(
        self,
        dataset: pd.DataFrame | Path | str | File,
        sampling_strategy: SamplingStrategy = SamplingStrategy.ORDERED,
        with_replacement: bool = False,
    ) -> Self:
        """Define a dataset to seed the synthetic data generation process.

        Each row of the seed dataset is treated as a single example. The columns
        of the seed dataset can be referenced by other columns in prompt templates
        and/or expressions.

        The seed data will be sampled using one of the following strategies:
            - "ordered": Maintains the order of the rows in the seed dataset.
            - "shuffle": Randomly shuffles the rows of the seed dataset.

        Args:
            dataset: DataFrame, Path, or File object.
            sampling_strategy: Sampling strategy to use.
            with_replacement: If True, the same row can be sampled multiple times.
        """
        if isinstance(dataset, File):
            file_id = dataset.id
            dataset_columns = self._retrieve_remote_dataset_columns(file_id)

        elif isinstance(dataset, str) and dataset.startswith("file_"):
            file_id = dataset
            dataset_columns = self._retrieve_remote_dataset_columns(file_id)

        else:
            df = smart_load_dataframe(dataset)
            file_id = self._files.upload(df, "dataset").id
            dataset_columns = df.columns.tolist()

        logger.info(f"🌱 Using seed dataset with file ID: {file_id}")

        for column in dataset_columns:
            self._columns[column] = DataSeedColumn(name=column, file_id=file_id)

        self._seed_dataset = SeedDataset(
            file_id=file_id,
            sampling_strategy=sampling_strategy,
            with_replacement=with_replacement,
        )

        return self

    def validate(self) -> Self:
        """Validate the current Data Designer configuration.

        This method runs task-level validation on the current configuration and
        "semantic" validation, which runs a wholistic check of the full schema for
        issues like references to undefined columns or inconsistent settings between
        related columns.

        Returns:
            The current Data Designer instance.
        """
        # Run task-level validation.
        self._build_workflow()
        # Run semantic validation on full schema.
        violations = self._run_semantic_validation()
        if len(violations) == 0:
            logger.info("Validation passed ✅")
        return self

    @property
    def seed_columns(self) -> list[DataSeedColumn]:
        """Columns from the seed dataset, if one is defined."""
        return self.get_columns_of_type(DataSeedColumn)

    @property
    def sampler_columns(self) -> list[SamplerColumn]:
        """Columns that use a sampler to generate data."""
        return self.get_columns_of_type(SamplerColumn)

    @property
    def llm_gen_columns(self) -> list[LLMGenColumn]:
        """Columns that use an LLM to generate data."""
        return self.get_columns_of_type(LLMGenColumn)

    @property
    def llm_text_columns(self) -> list[LLMTextColumn]:
        """Columns that use an LLM to generate text data."""
        return self.get_columns_of_type(LLMTextColumn)

    @property
    def llm_code_columns(self) -> list[LLMCodeColumn]:
        """Columns that use an LLM to generate code."""
        return self.get_columns_of_type(LLMCodeColumn)

    @property
    def llm_structured_columns(self) -> list[LLMStructuredColumn]:
        """Columns that use an LLM to generate structured data."""
        return self.get_columns_of_type(LLMStructuredColumn)

    @property
    def llm_judge_columns(self) -> list[LLMJudgeColumn]:
        """Columns that use an LLM to judge the quality of generated data."""
        return self.get_columns_of_type(LLMJudgeColumn)

    @property
    def code_validation_columns(self) -> list[CodeValidationColumn]:
        """Columns with results from validation of columns with code."""
        return self.get_columns_of_type(CodeValidationColumn)

    @property
    def expression_columns(self) -> list[ExpressionColumn]:
        """Columns that generate data from a jinja2 expression."""
        return self.get_columns_of_type(ExpressionColumn)

    @property
    def workflow_manager(self) -> WorkflowManager:
        """Workflow manager for the current Data Designer instance."""
        return self._workflow_manager

    @property
    def _dag_columns(self) -> list[DAGColumnT]:
        """Columns that are topologically sorted using a DAG."""
        return (
            self.llm_gen_columns
            + self.llm_judge_columns
            + self.code_validation_columns
            + self.expression_columns
        )

    @property
    def _categorical_columns(self) -> list[SamplerColumn]:
        """Columns that contain categorical data."""
        return [
            col
            for col in self.sampler_columns
            if (col.type == SamplerType.CATEGORY or col.type == SamplerType.SUBCATEGORY)
        ]

    @handle_workflow_validation_error
    def _build_workflow(
        self,
        num_records: int | None = None,
        verbose_logging: bool = False,
        streaming: bool = False,
    ) -> WorkflowBuilder:
        """Build a workflow from the current Data Designer configuration."""
        if self._seed_dataset is None and len(self.sampler_columns) == 0:
            raise ValueError(
                "🛑 Data Designer needs a seed dataset and/or at least one column that is "
                "generated using a non-LLM sampler. Seeding data is an essential ingredient "
                "for creating rich and diverse synthetic data."
            )

        num_records = (
            NUM_PREVIEW_RECORDS if streaming else num_records or NUM_PREVIEW_RECORDS
        )

        builder = self._workflow_manager.builder(
            globals=Globals(
                num_records=num_records,
                model_suite=self._model_suite,
                model_configs=self._model_configs,
            )
        )

        sample_from_dataset_step = None
        columns_using_samples_step = None
        last_step_added = None

        ###################################################
        # Add seed dataset to workflow if provided
        ###################################################

        if self._seed_dataset is not None:
            sample_from_dataset_step = self._task_registry.SampleFromDataset(
                num_samples=num_records,
                strategy=self._seed_dataset.sampling_strategy,
                with_replacement=self._seed_dataset.with_replacement,
            )
            builder.add_step(
                step=sample_from_dataset_step,
                step_inputs=[self._seed_dataset.file_id],
                step_name="seeding-workflow-with-dataset",
            )
            last_step_added = sample_from_dataset_step

        ########################################################
        # Add all sampler columns to workflow (single step)
        ########################################################

        if len(self.sampler_columns) > 0:
            columns_using_samples_step = (
                self._task_registry.GenerateColumnsUsingSamplers(
                    data_schema=DataSchema(
                        columns=[c for c in self.sampler_columns],
                        constraints=[c for c in list(self._constraints.values())],
                    ),
                    num_records=num_records,
                )
            )
            builder.add_step(
                step=columns_using_samples_step,
                step_inputs=[],
                step_name=f"using-samplers-to-generate-{len(self.sampler_columns)}-columns",
            )
            last_step_added = columns_using_samples_step

        ########################################################
        # Concatenate seed and sampler datasets (if both exist)
        ########################################################

        if (
            sample_from_dataset_step is not None
            and columns_using_samples_step is not None
        ):
            concat_step = self._task_registry.ConcatDatasets()
            builder.add_step(
                step=concat_step,
                step_inputs=[sample_from_dataset_step, columns_using_samples_step],
                step_name="concatenating-seed-and-sampler-datasets",
            )
            last_step_added = concat_step

        ########################################################
        # Add DAG columns to workflow (multiple steps)
        ########################################################

        sorted_columns_names = topologically_sort_columns(
            self._dag_columns, logger=logger if verbose_logging else None
        )

        for column_name in sorted_columns_names:
            column = self.get_column(column_name)
            next_step = self._get_next_dag_step(column_name)
            builder.add_step(
                step=next_step,
                step_inputs=[last_step_added],
                step_name=column.step_name,
            )
            last_step_added = next_step

        ########################################################
        # Drop all latent columns from the final dataset
        ########################################################

        if len(self._latent_person_columns) > 0:
            drop_latent_columns_step = self._task_registry.DropColumns(
                columns=list(self._latent_person_columns.keys()),
            )
            builder.add_step(
                step=drop_latent_columns_step,
                step_inputs=[last_step_added],
                step_name=(
                    f"dropping-{len(self._latent_person_columns)}-latent-person-"
                    f"column{'s' if len(self._latent_person_columns) > 1 else ''}"
                ),
            )
            last_step_added = drop_latent_columns_step

        ########################################################
        # Run dataset evaluation if requested
        ########################################################

        if self._evaluation_report is not None:
            settings = self._evaluation_report.settings
            if streaming:
                general_eval_step = self._task_registry.EvaluateDataset(
                    seed_columns=[col.name for col in self._categorical_columns],
                    # No longer available, we are passing in EvaluateDataDesignerDatasetSettings now
                    # other_list_like_columns=settings.list_like_columns,
                    # ordered_list_like_columns=settings.ordered_list_like_columns,
                    columns_to_ignore=settings.columns_to_ignore,
                )
            else:
                general_eval_step = self._task_registry.EvaluateDataDesignerDataset(
                    # TODO: Update to use all judge columns once the evaluate-dataset task is updated.
                    llm_judge_column=(
                        ""
                        if len(self.llm_judge_columns) == 0
                        else self.llm_judge_columns[0].name
                    ),
                    columns_to_ignore=settings.columns_to_ignore,
                    validation_columns=settings.validation_columns,
                    defined_categorical_columns=[
                        c.name for c in self._categorical_columns
                    ],
                )
            builder.add_step(
                step=general_eval_step,
                step_inputs=[last_step_added],
                step_name="evaluating-dataset",
            )
            last_step_added = general_eval_step

        return builder

    def _capture_preview_result(
        self, workflow: WorkflowBuilder, verbose_logging: bool
    ) -> PreviewResults:
        """Capture the results (including logs) of a workflow preview."""
        step_idx = 0
        message: Message
        current_step = None
        final_output = None
        outputs_by_step = {}
        success = True
        column_names = list(self._columns.keys())
        for message in workflow.iter_preview():
            if isinstance(message, WorkflowInterruption):
                success = False
                logger.error(message.message)
                break
            if not message.step:
                continue
            if current_step != message.step:
                current_step = message.step
                log_name = _add_backticks_to_column_names(message.step, column_names)
                logger.info(
                    f"{get_task_log_emoji(log_name)}Step {step_idx + 1}: "
                    f"{log_name.replace('-', ' ').capitalize()}"
                )
                step_idx += 1

            if message.has_log_message:
                log_msg = message.log_message

                if (log_msg.is_info and verbose_logging) or (
                    log_msg.is_error or log_msg.is_warning
                ):
                    formatted_msg = (
                        f"  {'|' if '|--' in log_msg.msg else '|--'} {log_msg.msg}"
                    )
                    if log_msg.is_info:
                        logger.info(formatted_msg)
                    elif log_msg.is_warning:
                        logger.warning(formatted_msg)
                    else:
                        success = False
                        logger.error(formatted_msg)

            if message.has_output:
                logger.debug(f"Step output: {json.dumps(message.payload, indent=4)}")

                output = message.payload
                if message.has_dataset:
                    final_output = message.dataset
                outputs_by_step[message.step] = output
        # the final output is either the dataset produced by the last
        # task in the workflow, or, if no dataset is produced by the workflow
        # the final output will be the output of the last task to complete
        # (which may also be none)
        last_evaluation_step_name = self._get_last_evaluation_step_name(
            workflow_step_names=workflow.step_names
        )
        if final_output is None:
            final_output = outputs_by_step.get(current_step)
        evaluation_results = (
            None
            if last_evaluation_step_name is None
            else outputs_by_step.get(last_evaluation_step_name)
        )
        return PreviewResults(
            output=final_output,
            evaluation_results=evaluation_results,
            aidd_metadata=AIDDMetadata.from_aidd(self),
            success=success,
        )

    def _get_next_dag_step(self, column_name: str) -> TaskConfig:
        """Return the task for the given column for the next step in the DAG."""
        column = self.get_column(column_name)
        if isinstance(column, LLMGenColumn):
            next_step = self._task_registry.GenerateColumnFromTemplateV2(
                **column.model_dump(**MODEL_DUMP_KWARGS)
            )
        elif isinstance(column, LLMJudgeColumn):
            next_step = self._task_registry.JudgeWithLlm(
                **column.model_dump(**MODEL_DUMP_KWARGS)
            )
        elif isinstance(column, CodeValidationColumn):
            next_step = self._task_registry.ValidateCode(
                code_lang=CodeLang(column.code_lang),
                target_columns=[column.target_column],
                result_columns=[column.name],
            )
        elif isinstance(column, ExpressionColumn):
            next_step = self._task_registry.GenerateColumnFromExpression(
                name=column.name,
                expr=column.expr,
                dtype=ExprDtype(column.dtype),
            )
        else:
            raise ValueError(f"🛑 Columns of type {type(column)} do not go in the DAG.")
        return next_step

    def _retrieve_remote_dataset_columns(self, file_id: str) -> list[str]:
        """Return the columns of a dataset given its file id."""
        retrieved_df = self._files.download_dataset(file_id)
        return retrieved_df.columns.tolist()

    def _run_semantic_validation(
        self, raise_exceptions: bool = False
    ) -> list[Violation]:
        """Run semantic validation on the current Data Designer configuration."""
        violations = validate_aidd_columns(
            columns=list(self._columns.values()),
            allowed_references=self.allowed_references,
        )
        rich_print_violations(violations)
        if (
            raise_exceptions
            and len([v for v in violations if v.level == ViolationLevel.ERROR]) > 0
        ):
            raise DataDesignerValidationError(
                "🛑 Your dataset contains validation errors. "
                "Please address the indicated issues and try again."
            )
        return violations

    @staticmethod
    def _get_last_evaluation_step_name(workflow_step_names: list[str]) -> str | None:
        """Return the name of the last evaluation step in a workflow."""
        eval_steps = [
            s for s in workflow_step_names if s.startswith("evaluate-dataset")
        ]
        return None if len(eval_steps) == 0 else eval_steps[-1]

    def __repr__(self) -> str:
        model_suite = (
            self.model_suite.value
            if isinstance(self.model_suite, ModelSuite)
            else self.model_suite
        )

        if len(self._columns) == 0:
            return f"{self.__class__.__name__}(model_suite: {model_suite})"

        md = AIDDMetadata.from_aidd(self)
        props_to_repr = {
            "model_suite": model_suite,
            "person_samplers": _check_convert_to_json_str(md.person_samplers),
            "seed_dataset": (
                None if self._seed_dataset is None else self._seed_dataset.file_id
            ),
        }

        for name in [
            "seed_columns",
            "sampler_columns",
            "llm_text_columns",
            "llm_code_columns",
            "llm_structured_columns",
            "llm_judge_columns",
            "validation_columns",
            "expression_columns",
        ]:
            props_to_repr[name] = _check_convert_to_json_str(
                getattr(md, name), indent=8
            )

        repr_string = f"{self.__class__.__name__}(\n"
        for k, v in props_to_repr.items():
            if v is not None:
                v_indented = v if "[" not in v else f"{v[:-1]}" + "    " + v[-1]
                repr_string += f"    {k}: {v_indented}\n"
        repr_string += ")"
        return repr_string

    def _repr_html_(self) -> str:
        repr_string = self.__repr__()
        formatter = HtmlFormatter(style=self._repr_html_style, cssclass="code")
        highlighted_html = highlight(repr_string, PythonLexer(), formatter)
        css = formatter.get_style_defs(".code")
        return REPR_HTML_TEMPLATE.format(css=css, highlighted_html=highlighted_html)


def get_column_from_kwargs(
    name: str, type: ColumnProviderTypeT, **kwargs
) -> AIDDColumnT:
    """Create a concrete AIDD column object from kwargs.

    Args:
        name: Name of the column.
        type: Type of the column.
        **kwargs: Keyword arguments to pass to the column constructor.

    Returns:
        AIDD column object of the appropriate type.
    """
    if name is None or type is None:
        raise ValueError(
            "You must provide both `name` and `type` to add a column using kwargs."
        )
    column_klass = None
    match type:
        case ProviderType.LLM_TEXT:
            column_klass = LLMTextColumn
        case ProviderType.LLM_CODE:
            column_klass = LLMCodeColumn
        case ProviderType.LLM_STRUCTURED:
            column_klass = LLMStructuredColumn
        case ProviderType.LLM_JUDGE:
            column_klass = LLMJudgeColumn
        case ProviderType.CODE_VALIDATION:
            column_klass = CodeValidationColumn
        case ProviderType.EXPRESSION:
            column_klass = ExpressionColumn
        case _:
            kwargs["params"] = _SAMPLER_PARAMS[type](**kwargs.get("params", {}))
            kwargs["type"] = type
            column_klass = SamplerColumn
    return column_klass(name=name, **kwargs)


def _add_backticks_to_column_names(step_name: str, column_names: list[str]) -> str:
    """Add backticks to the column names in the step name if they are present.

    This function is used in the context of logging workflow steps.

    Args:
        step_name: Name of the step.
        column_names: List of possible column names.

    Returns:
        Step name with backticks added to the column names if present.
    """
    max_overlap = 0
    best_match = None
    for name in column_names:
        if name in step_name:
            overlap = len(name)
            if overlap > max_overlap:
                max_overlap = overlap
                best_match = name
    if best_match:
        step_name = step_name.replace(best_match, f"`{best_match}`")
    return step_name


def _check_convert_to_json_str(
    column_names: list[str], *, indent: int | str | None = None
) -> list[str] | str | None:
    """Convert a list of column names to a JSON string if the list is long.

    This function helps keep AIDD's __repr__ output clean and readable.

    Args:
        column_names: List of column names.
        indent: Indentation for the JSON string.

    Returns:
        List of column names or a JSON string if the list is long.
    """
    return (
        None
        if len(column_names) == 0
        else (
            column_names
            if len(column_names) < REPR_LIST_LENGTH_USE_JSON
            else json.dumps(column_names, indent=indent)
        )
    )


def _validate_column_provider_type(column_provider_type: str) -> ColumnProviderTypeT:
    """Validate the given column provider type and return the appropriate enum."""
    valid_provider_types = {t.value for t in list(ProviderType)}
    valid_sampling_source_types = {t.value for t in list(SamplerType)}
    combined_valid_types = valid_provider_types.union(valid_sampling_source_types)
    if column_provider_type not in combined_valid_types:
        raise ValueError(
            f"🛑 Invalid column provider type: '{column_provider_type}'. "
            f"Valid options are: {list(combined_valid_types)}"
        )
    elif column_provider_type in valid_provider_types:
        return ProviderType(column_provider_type)
    else:
        return SamplerType(column_provider_type)
