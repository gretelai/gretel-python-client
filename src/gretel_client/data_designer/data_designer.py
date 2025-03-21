import json
import logging

from pathlib import Path
from typing import Any, cast, Type

import pandas as pd
import yaml

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer
from typing_extensions import Self

from gretel_client.data_designer.aidd_config import AIDDConfig
from gretel_client.data_designer.constants import (
    DEFAULT_REPR_HTML_STYLE,
    REPR_HTML_TEMPLATE,
    SQL_DIALECTS,
    VALIDATE_PYTHON_COLUMN_SUFFIXES,
    VALIDATE_SQL_COLUMN_SUFFIXES,
)
from gretel_client.data_designer.log import get_logger
from gretel_client.data_designer.preview import PreviewResults
from gretel_client.data_designer.types import (
    DataColumnFromCodeValidation,
    DataColumnFromJudge,
    DataColumnFromPrompt,
    DataColumnFromSamplingT,
    DataColumnT,
    DataPipelineMetadata,
    EvaluateDatasetSettings,
    EvaluationReportT,
    GeneralDatasetEvaluation,
    ModelSuite,
    NonSamplingSupportedTypes,
    SeedDataset,
    SupportedColumnTypesT,
)
from gretel_client.data_designer.utils import (
    camel_to_kebab,
    fetch_config_if_remote,
    get_task_log_emoji,
    make_date_obj_serializable,
)
from gretel_client.files.interface import File
from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.workflows.builder import (
    Message,
    WorkflowBuilder,
    WorkflowInterruption,
)
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.tasks import (
    ColumnConstraint,
    ConstraintType,
    DataSchema,
    PersonSamplerParams,
    SamplingSourceType,
    SamplingStrategy,
)
from gretel_client.workflows.configs.workflows import Globals, ModelConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = get_logger(__name__, level=logging.INFO)


class DataDesigner:

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
        columns = {col.name: col for col in valid_config.columns}
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
        columns: dict[str, DataColumnT] | None = None,
        constraints: dict[str, ColumnConstraint] | None = None,
        evaluation_report: EvaluationReportT | None = None,
    ):
        self._gretel_resource_provider = gretel_resource_provider
        self._model_suite = model_suite
        self._model_configs = model_configs
        self._seed_dataset = seed_dataset
        self._columns = columns or {}
        self._constraints = constraints or {}
        self._evaluation_report = evaluation_report or {}
        self._task_registry = Registry()
        self._files = self._gretel_resource_provider.files
        self._workflow_manager = self._gretel_resource_provider.workflows
        self._repr_html_style = DEFAULT_REPR_HTML_STYLE
        self._latent_columns: dict[str, PersonSamplerParams] = {}
        if person_samplers:
            self.with_person_samplers(person_samplers)

    @property
    def model_suite(self) -> ModelSuite:
        return self._model_suite

    @property
    def model_configs(self) -> list[ModelConfig] | None:
        return self._model_configs

    def get_column(self, column_name: str) -> DataColumnT | None:
        return self._columns.get(column_name, None)

    def get_columns_of_type(self, column_type: Type) -> list[DataColumnT]:
        return [col for col in self._columns.values() if isinstance(col, column_type)]

    def delete_column(self, column_name: str) -> Self:
        self._columns.pop(column_name, None)
        return self

    def add_column(
        self,
        name: str,
        type: SupportedColumnTypesT = NonSamplingSupportedTypes.LLM_GENERATED,
        **kwargs,
    ) -> Self:
        kwargs_set = set(list(kwargs.keys()) + ["name", "type"])
        self._validate_add_column_kwargs(type, kwargs_set)
        column_klass = None
        match type:
            case NonSamplingSupportedTypes.LLM_GENERATED:
                column_klass = DataColumnFromPrompt
            case NonSamplingSupportedTypes.LLM_JUDGE:
                column_klass = DataColumnFromJudge
            case NonSamplingSupportedTypes.CODE_VALIDATION:
                column_klass = DataColumnFromCodeValidation
            case _:
                column_klass = DataColumnFromSamplingT
        return self._add_column(column=column_klass(name=name, type=type, **kwargs))

    def get_constraint(self, target_column: str) -> ColumnConstraint | None:
        return self._constraints.get(target_column, None)

    def delete_constraint(self, target_column: str) -> Self:
        self._constraints.pop(target_column, None)
        return self

    def add_constraint(
        self, target_column: str, type: ConstraintType, params: dict[str, str | float]
    ) -> Self:
        self._constraints[target_column] = ColumnConstraint(
            target_column=target_column,
            type=type,
            params=params,
        )
        return self

    def get_evaluation_report(self) -> EvaluationReportT | None:
        return self._evaluation_report

    def delete_evaluation_report(self) -> Self:
        self._evaluation_report = None
        return self

    def with_evaluation_report(
        self, settings: EvaluateDatasetSettings | None = None
    ) -> Self:
        self._evaluation_report = GeneralDatasetEvaluation(settings=settings or {})
        return self

    def preview(self, verbose_logging: bool = True) -> PreviewResults:
        logger.info("🚀 Generating preview")
        workflow = self._build_workflow(num_records=10, verbose_logging=verbose_logging)

        preview = self._capture_preview_result(
            workflow, verbose_logging=verbose_logging
        )
        if preview.dataset is not None:
            logger.info("👀 Your dataset preview is ready for a peek!")

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
        workflow_run_name: str,
        wait_for_completion: bool = False,
    ) -> WorkflowRun:
        logger.info("🚀 Submitting batch workflow")
        workflow = self._build_workflow(num_records=num_records)
        return workflow.run(name=workflow_run_name, wait=wait_for_completion)

    def to_aidd_config(self) -> AIDDConfig:
        return AIDDConfig(
            model_suite=self.model_suite,
            model_configs=self.model_configs,
            seed_dataset=self._seed_dataset,
            columns=list(self._columns.values()),
            constraints=list(self._constraints.values()),
            evaluation_report=self._evaluation_report,
        )

    def to_config_dict(self) -> dict[str, Any]:
        return self.to_aidd_config().model_dump(mode="json")

    def export_config(self, path: str | Path) -> None:
        config_dict = self.to_config_dict()
        path = Path(path)
        match path.suffix.lower():
            case ".yaml" | ".yml":
                with open(path, "w") as f:
                    yaml.dump(config_dict, f, sort_keys=False)
            case ".json":
                with open(path, "w") as f:
                    json.dump(config_dict, f, sort_keys=False, indent=4)
            case _:
                raise ValueError(
                    "The file extension must be one of .yaml, .yml, or .json."
                    f"You provided: {path.suffix}"
                )

    def with_person_samplers(
        self, person_samplers: dict[str, PersonSamplerParams]
    ) -> Self:
        for name, params in person_samplers.items():
            person_params = PersonSamplerParams.model_validate(params)
            self._add_column(
                DataColumnFromSamplingT(
                    name=name,
                    type=SamplingSourceType.PERSON,
                    params=person_params.model_dump(),
                )
            )
            self._latent_columns[name] = person_params
        return self

    def with_seed_dataset(
        self,
        dataset: pd.DataFrame | Path | str,
        sampling_strategy: SamplingStrategy = SamplingStrategy.ORDERED,
        with_replacement: bool = False,
    ) -> Self:
        if isinstance(dataset, File):
            file_id = dataset.id

        elif isinstance(dataset, str) and dataset.startswith("file_"):
            file_id = dataset

        else:
            file_id = self._files.upload(dataset, "dataset").id

        logger.info(f"🌱 Using seed dataset with file ID: {file_id}")

        self._seed_dataset = SeedDataset(
            file_id=file_id,
            sampling_strategy=sampling_strategy,
            with_replacement=with_replacement,
        )
        return self

    @property
    def _columns_from_sampling(self) -> list[DataColumnFromSamplingT]:
        return cast(
            list[DataColumnFromSamplingT],
            self.get_columns_of_type(DataColumnFromSamplingT),
        )

    @property
    def _columns_from_prompt(self) -> list[DataColumnFromPrompt]:
        return cast(
            list[DataColumnFromPrompt], self.get_columns_of_type(DataColumnFromPrompt)
        )

    @property
    def _columns_from_judge(self) -> list[DataColumnFromJudge]:
        return cast(
            list[DataColumnFromJudge], self.get_columns_of_type(DataColumnFromJudge)
        )

    @property
    def _columns_from_code_validation(self) -> list[DataColumnFromCodeValidation]:
        return cast(
            list[DataColumnFromCodeValidation],
            self.get_columns_of_type(DataColumnFromCodeValidation),
        )

    @property
    def _columns_from_categorical_sampling(self) -> list[DataColumnFromSamplingT]:
        return [
            col
            for col in self._columns_from_sampling
            if (
                col.type == SamplingSourceType.CATEGORY
                or col.type == SamplingSourceType.SUBCATEGORY
            )
        ]

    def _add_column(self, column: DataColumnT) -> Self:
        self._columns[column.name] = column
        return self

    def _build_workflow(
        self, num_records: int, verbose_logging: bool = False
    ) -> WorkflowBuilder:

        if self._seed_dataset is None and len(self._columns_from_sampling) == 0:
            raise ValueError(
                "🛑 Data Designer needs a seed dataset and/or at least one column that is "
                "generated using a non-LLM sampler. Seeding data is an essential ingredient "
                "for creating rich and diverse synthetic data."
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
        if self._seed_dataset is not None:
            sample_from_dataset_step = self._task_registry.SampleFromDataset(
                num_samples=num_records,
                strategy=self._seed_dataset.sampling_strategy,
                with_replacement=self._seed_dataset.with_replacement,
            )
            builder.add_step(
                step=sample_from_dataset_step, step_inputs=[self._seed_dataset.file_id]
            )
            last_step_added = sample_from_dataset_step

        if len(self._columns_from_sampling) > 0:
            columns_using_samples_step = (
                self._task_registry.GenerateColumnsUsingSamplers(
                    data_schema=DataSchema(
                        columns=[c for c in self._columns_from_sampling],
                        constraints=[c for c in list(self._constraints.values())],
                    ),
                    num_records=num_records,
                )
            )
            builder.add_step(step=columns_using_samples_step, step_inputs=[])
            last_step_added = columns_using_samples_step

        if (
            sample_from_dataset_step is not None
            and columns_using_samples_step is not None
        ):
            concat_step = self._task_registry.ConcatDatasets()
            builder.add_step(
                step=concat_step,
                step_inputs=[sample_from_dataset_step, columns_using_samples_step],
            )
            last_step_added = concat_step

        for prompt_based_column in self._columns_from_prompt:
            generate_column_from_template_args = prompt_based_column.model_dump(
                exclude={"type"}
            )
            columns_from_prompt_step = self._task_registry.GenerateColumnFromTemplate(
                **generate_column_from_template_args
            )
            builder.add_step(
                step=columns_from_prompt_step, step_inputs=[last_step_added]
            )
            last_step_added = columns_from_prompt_step

        for validation_column in self._columns_from_code_validation:
            validation_step = self._task_registry.ValidateCode(
                code_lang=validation_column.code_lang,
                target_columns=[validation_column.target_column],
                result_columns=[validation_column.name],
            )
            builder.add_step(step=validation_step, step_inputs=[last_step_added])
            last_step_added = validation_step

        last_dataset_step = last_step_added

        for judge_based_column in self._columns_from_judge:
            judge_with_llm_args = judge_based_column.model_dump(exclude={"type"})
            judge_with_llm_args["result_column"] = judge_with_llm_args.pop("name")
            judge_step = self._task_registry.JudgeWithLlm(**judge_with_llm_args)
            builder.add_step(step=judge_step, step_inputs=[last_step_added])
            last_step_added = judge_step

        if self._evaluation_report is not None:
            settings = self._evaluation_report.settings
            general_eval_step = self._task_registry.EvaluateDataset(
                seed_columns=[
                    col.name for col in self._columns_from_categorical_sampling
                ],
                other_list_like_columns=settings.list_like_columns,
                ordered_list_like_columns=settings.ordered_list_like_columns,
                # TODO: Update to use all judge columns once the evaluate-dataset task is updated.
                llm_judge_column=(
                    ""
                    if len(self._columns_from_judge) == 0
                    else self._columns_from_judge[0].name
                ),
                columns_to_ignore=settings.columns_to_ignore,
            )
            builder.add_step(
                step=general_eval_step,
                step_inputs=[last_step_added],
            )
            last_step_added = general_eval_step

        if len(self._latent_columns) > 0:
            drop_latent_columns_step = self._task_registry.DropColumns(
                columns=list(self._latent_columns.keys()),
            )
            builder.add_step(
                step=drop_latent_columns_step, step_inputs=[last_dataset_step]
            )
            last_step_added = drop_latent_columns_step

        if verbose_logging:
            self._write_workflow_steps_to_logger(builder.get_steps())

        return builder

    def _capture_preview_result(
        self, workflow: WorkflowBuilder, verbose_logging: bool
    ) -> PreviewResults:
        step_idx = 0
        message: Message
        current_step = None
        final_output = None
        outputs_by_step = {}
        for message in workflow.iter_preview():
            if isinstance(message, WorkflowInterruption):
                logger.warning(message.message)
                break
            if not message.step:
                continue
            if current_step != message.step:
                current_step = message.step
                task_name = message.step
                step_name = task_name.replace("-" + str(step_idx + 1), "")
                label = (
                    ""
                    if task_name == step_name
                    else f" >>{step_name.split(task_name)[-1].replace('-', ' ')}"
                )
                logger.info(
                    f"{get_task_log_emoji(task_name)}Step {step_idx + 1}: "
                    f"{task_name.replace('-', ' ').capitalize()}{label}"
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
            data_pipeline_metadata=self._get_data_pipeline_metadata(),
        )

    def _get_data_pipeline_metadata(
        self,
    ) -> DataPipelineMetadata:
        """Return a DataPipelineMetadata instance that defines the schema and other relevant info."""
        code_lang = None
        code_columns = []
        code_validation_columns = []
        llm_judge_columns = []
        eval_type = None

        for code_val_col in self._columns_from_code_validation:
            code_validation_columns.append(code_val_col.name)

            column_suffix = (
                VALIDATE_SQL_COLUMN_SUFFIXES
                if code_val_col.code_lang in SQL_DIALECTS
                else VALIDATE_PYTHON_COLUMN_SUFFIXES
            )

            for suffix in column_suffix:
                code_validation_columns.append(f"{code_val_col.target_column}{suffix}")

        sampling_based_columns = [
            col.name
            for col in self._columns_from_sampling
            if col.name not in list(self._latent_columns.keys())
        ]
        prompt_based_columns = [col.name for col in self._columns_from_prompt]
        llm_judge_columns = [col.name for col in self._columns_from_judge]
        return DataPipelineMetadata(
            sampling_based_columns=sampling_based_columns,
            prompt_based_columns=prompt_based_columns,
            llm_judge_columns=llm_judge_columns,
            validation_columns=code_validation_columns,
            code_column_names=code_columns,
            code_lang=code_lang,
            eval_type=eval_type,
        )

    def _write_workflow_steps_to_logger(self, steps: list[Any]) -> None:
        logger.info("⚙️ Configuring Data Designer Workflow steps:")
        for i, step in enumerate(steps):
            step_name = camel_to_kebab(step.task)
            suffix = ""
            if isinstance(step, self._task_registry.GenerateColumnsUsingSamplers):
                suffix = f"-generating {', '.join([col.name for col in self._columns_from_sampling])}"
            elif isinstance(step, self._task_registry.GenerateColumnFromTemplate):
                suffix = f"-generating {step.name}"
            name = f"{step_name}-{i + 1}{suffix}".replace(",", "").replace(" ", "-")
            logger.info(f"  |-- Step {i + 1}: {name}")

    def _validate_add_column_kwargs(
        self, type: SupportedColumnTypesT, kwargs_set: set[str]
    ) -> None:
        valid_kwargs = {}
        if type == NonSamplingSupportedTypes.LLM_GENERATED:
            valid_kwargs = set(DataColumnFromPrompt.model_fields.keys())
            invalid_kwargs = kwargs_set - valid_kwargs
        elif type == NonSamplingSupportedTypes.LLM_JUDGE:
            valid_kwargs = set(DataColumnFromJudge.model_fields.keys())
            invalid_kwargs = kwargs_set - valid_kwargs
        elif type == NonSamplingSupportedTypes.CODE_VALIDATION:
            valid_kwargs = set(DataColumnFromCodeValidation.model_fields.keys())
            invalid_kwargs = kwargs_set - valid_kwargs
        else:
            valid_kwargs = set(DataColumnFromSamplingT.model_fields.keys())
            invalid_kwargs = kwargs_set - valid_kwargs
        if len(invalid_kwargs) > 0:
            raise ValueError(
                f"Invalid keyword arguments {invalid_kwargs}. Arguments must map to one of {valid_kwargs}"
            )

    @staticmethod
    def _get_last_evaluation_step_name(workflow_step_names: list[str]) -> str | None:
        eval_steps = [
            s for s in workflow_step_names if s.startswith("evaluate-dataset")
        ]
        return None if len(eval_steps) == 0 else eval_steps[-1]

    def __repr__(self) -> str:
        max_list_elements = 3

        model_suite = (
            self.model_suite.value
            if isinstance(self.model_suite, ModelSuite)
            else self.model_suite
        )

        if len(self._columns) == 0:
            return f"{self.__class__.__name__}(model_suite: {model_suite})"

        md = self._get_data_pipeline_metadata()

        props_to_repr = {
            "model_suite": model_suite,
            "seed_dataset": (
                None if self._seed_dataset is None else self._seed_dataset.file_id
            ),
            "person_samplers": (
                None
                if len(self._latent_columns) == 0
                else list(self._latent_columns.keys())
            ),
            "sampling_based_columns": (
                None
                if len(md.sampling_based_columns) == 0
                else (
                    json.dumps(md.sampling_based_columns, indent=8)
                    if len(md.sampling_based_columns) > max_list_elements
                    else md.sampling_based_columns
                )
            ),
            "llm_based_columns": (
                None
                if len(md.prompt_based_columns) == 0
                else (
                    json.dumps(md.prompt_based_columns, indent=8)
                    if len(md.prompt_based_columns) > max_list_elements
                    else md.prompt_based_columns
                )
            ),
            "llm_judge_columns": (
                None
                if len(md.llm_judge_columns) == 0
                else (
                    json.dumps(md.llm_judge_columns, indent=8)
                    if len(md.llm_judge_columns) > max_list_elements
                    else md.llm_judge_columns
                )
            ),
            "validation_columns": (
                None
                if len(md.validation_columns) == 0
                else (
                    json.dumps(md.validation_columns, indent=8)
                    if len(md.validation_columns) > max_list_elements
                    else md.validation_columns
                )
            ),
        }

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
