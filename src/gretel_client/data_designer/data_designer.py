import json
import logging

from pathlib import Path
from typing import Any, Type

import pandas as pd
import yaml

from pydantic import BaseModel
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer
from typing_extensions import Self

from gretel_client.data_designer.aidd_config import AIDDConfig
from gretel_client.data_designer.constants import (
    DEFAULT_REPR_HTML_STYLE,
    MODEL_DUMP_KWARGS,
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
    EvaluateDatasetSettings,
    EvaluationReportT,
    ExpressionColumn,
    GeneralDatasetEvaluation,
    LLMGenColumn,
    LLMJudgeColumn,
    ModelSuite,
    ProviderType,
    SamplerColumn,
    SeedDataset,
)
from gretel_client.data_designer.utils import (
    fetch_config_if_remote,
    get_sampler_params,
    get_task_log_emoji,
    make_date_obj_serializable,
    smart_load_dataframe,
)
from gretel_client.data_designer.viz_tools import AIDDMetadata
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
    CodeLang,
    ColumnConstraint,
    ConstraintType,
    DataSchema,
)
from gretel_client.workflows.configs.tasks import Dtype as ExprDtype
from gretel_client.workflows.configs.tasks import (
    PersonSamplerParams,
    SamplingSourceType,
    SamplingStrategy,
)
from gretel_client.workflows.configs.workflows import Globals, ModelConfig
from gretel_client.workflows.tasks import TaskConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = get_logger(__name__, level=logging.INFO)


_type_builtin = type
_SAMPLER_PARAMS: dict[SamplingSourceType, Type[BaseModel]] = get_sampler_params()


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
        columns: dict[str, AIDDColumnT] | None = None,
        constraints: dict[str, ColumnConstraint] | None = None,
        evaluation_report: EvaluationReportT | None = None,
    ):
        self._gretel_resource_provider = gretel_resource_provider
        self._model_suite = model_suite
        self._model_configs = model_configs
        self._seed_dataset = seed_dataset
        self._columns = columns or {}
        self._constraints = constraints or {}
        self._evaluation_report = evaluation_report
        self._task_registry = Registry()
        self._files = self._gretel_resource_provider.files
        self._workflow_manager = self._gretel_resource_provider.workflows
        self._repr_html_style = DEFAULT_REPR_HTML_STYLE
        self._latent_columns: dict[str, PersonSamplerParams] = {}
        self.magic = MagicDataDesignerEditor(self)
        if person_samplers:
            self.with_person_samplers(person_samplers)

    @property
    def model_suite(self) -> ModelSuite:
        return self._model_suite

    @property
    def model_configs(self) -> list[ModelConfig] | None:
        return self._model_configs

    def get_column(self, column_name: str) -> AIDDColumnT | None:
        return self._columns.get(column_name, None)

    def get_columns_of_type(self, column_type: Type) -> list[AIDDColumnT]:
        return [col for col in self._columns.values() if isinstance(col, column_type)]

    def delete_column(self, column_name: str) -> Self:
        if isinstance(self._columns.get(column_name), DataSeedColumn):
            raise ValueError(
                "Seed columns cannot be deleted. Please update the seed dataset instead."
            )
        self._columns.pop(column_name, None)
        self.magic.reset()
        return self

    def add_column(
        self,
        column: AIDDColumnT | None = None,
        *,
        name: str | None = None,
        type: ColumnProviderTypeT = ProviderType.LLM_GEN,
        **kwargs,
    ) -> Self:
        if column is None:
            column = self._get_column_from_kwargs(name=name, type=type, **kwargs)
        if not isinstance(column, AIDDColumnT):
            raise ValueError(
                f"{_type_builtin(column)} is not a valid column type. "
                f"Columns must be one of {[t.__name__ for t in AIDDColumnT.__args__]}."
            )
        self._columns[column.name] = column
        self.magic.reset()
        return self

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

    def preview(self, verbose_logging: bool = False) -> PreviewResults:
        logger.info("🚀 Generating preview")
        workflow = self._build_workflow(num_records=10, verbose_logging=verbose_logging)

        preview = self._capture_preview_result(
            workflow, verbose_logging=verbose_logging
        )
        if preview.dataset is not None and preview.success:
            logger.info("🎉 Your dataset preview is ready!")
        else:
            logger.warning(
                "⚠️ Something has gone wrong during preview generation. Please inspect "
                "the generated data and adjust your configuration as needed. If the issue "
                "persists please contact support."
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
        self,
        person_samplers: dict[str, PersonSamplerParams],
        *,
        keep_person_columns: bool = False,
    ) -> Self:
        for name, params in person_samplers.items():
            person_params = PersonSamplerParams.model_validate(params)
            self.add_column(
                SamplerColumn(
                    name=name,
                    type=SamplingSourceType.PERSON,
                    params=person_params.model_dump(),
                )
            )
            if not keep_person_columns:
                self._latent_columns[name] = person_params
        return self

    def with_seed_dataset(
        self,
        dataset: pd.DataFrame | Path | str | File,
        sampling_strategy: SamplingStrategy = SamplingStrategy.ORDERED,
        with_replacement: bool = False,
    ) -> Self:
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

    @property
    def _seed_columns(self) -> list[DataSeedColumn]:
        return self.get_columns_of_type(DataSeedColumn)

    @property
    def _sampler_columns(self) -> list[SamplerColumn]:
        return self.get_columns_of_type(SamplerColumn)

    @property
    def _llm_gen_columns(self) -> list[LLMGenColumn]:
        return self.get_columns_of_type(LLMGenColumn)

    @property
    def _llm_judge_columns(self) -> list[LLMJudgeColumn]:
        return self.get_columns_of_type(LLMJudgeColumn)

    @property
    def _code_validation_columns(self) -> list[CodeValidationColumn]:
        return self.get_columns_of_type(CodeValidationColumn)

    @property
    def _expression_columns(self) -> list[ExpressionColumn]:
        return self.get_columns_of_type(ExpressionColumn)

    @property
    def _dag_columns(self) -> list[DAGColumnT]:
        return (
            self._llm_gen_columns
            + self._llm_judge_columns
            + self._code_validation_columns
            + self._expression_columns
        )

    @property
    def _categorical_columns(self) -> list[SamplerColumn]:
        return [
            col
            for col in self._sampler_columns
            if (
                col.type == SamplingSourceType.CATEGORY
                or col.type == SamplingSourceType.SUBCATEGORY
            )
        ]

    def _build_workflow(
        self, num_records: int, verbose_logging: bool = False
    ) -> WorkflowBuilder:

        if self._seed_dataset is None and len(self._sampler_columns) == 0:
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
                step=sample_from_dataset_step, step_inputs=[self._seed_dataset.file_id]
            )
            last_step_added = sample_from_dataset_step

        ########################################################
        # Add all sampler columns to workflow (single step)
        ########################################################

        if len(self._sampler_columns) > 0:
            columns_using_samples_step = (
                self._task_registry.GenerateColumnsUsingSamplers(
                    data_schema=DataSchema(
                        columns=[c for c in self._sampler_columns],
                        constraints=[c for c in list(self._constraints.values())],
                    ),
                    num_records=num_records,
                )
            )
            builder.add_step(step=columns_using_samples_step, step_inputs=[])
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
            )
            last_step_added = concat_step

        ########################################################
        # Add DAG columns to workflow (multiple steps)
        ########################################################

        sorted_columns_names = topologically_sort_columns(
            self._dag_columns, logger=logger if verbose_logging else None
        )

        for column_name in sorted_columns_names:
            next_step = self._get_next_dag_step(column_name)
            builder.add_step(step=next_step, step_inputs=[last_step_added])
            last_step_added = next_step

        ########################################################
        # Drop all latent columns from the final dataset
        ########################################################

        if len(self._latent_columns) > 0:
            drop_latent_columns_step = self._task_registry.DropColumns(
                columns=list(self._latent_columns.keys()),
            )
            builder.add_step(
                step=drop_latent_columns_step, step_inputs=[last_step_added]
            )
            last_step_added = drop_latent_columns_step

        ########################################################
        # Run dataset evaluation if requested
        ########################################################

        if self._evaluation_report is not None:
            settings = self._evaluation_report.settings
            general_eval_step = self._task_registry.EvaluateDataset(
                seed_columns=[col.name for col in self._categorical_columns],
                other_list_like_columns=settings.list_like_columns,
                ordered_list_like_columns=settings.ordered_list_like_columns,
                # TODO: Update to use all judge columns once the evaluate-dataset task is updated.
                llm_judge_column=(
                    ""
                    if len(self._llm_judge_columns) == 0
                    else self._llm_judge_columns[0].name
                ),
                columns_to_ignore=settings.columns_to_ignore,
            )
            builder.add_step(
                step=general_eval_step,
                step_inputs=[last_step_added],
            )
            last_step_added = general_eval_step

        return builder

    def _capture_preview_result(
        self, workflow: WorkflowBuilder, verbose_logging: bool
    ) -> PreviewResults:
        step_idx = 0
        message: Message
        current_step = None
        final_output = None
        outputs_by_step = {}
        success = True
        for message in workflow.iter_preview():
            if isinstance(message, WorkflowInterruption):
                success = False
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
                        success = False
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

    def _get_column_from_kwargs(
        self, name: str, type: ColumnProviderTypeT, **kwargs
    ) -> AIDDColumnT:
        if name is None or type is None:
            raise ValueError(
                "You must provide both `name` and `type` to add a column using kwargs."
            )
        match type:
            case ProviderType.LLM_GEN:
                column = LLMGenColumn(name=name, **kwargs)
            case ProviderType.LLM_JUDGE:
                column = LLMJudgeColumn(name=name, **kwargs)
            case ProviderType.CODE_VALIDATION:
                column = CodeValidationColumn(name=name, **kwargs)
            case ProviderType.EXPRESSION:
                column = ExpressionColumn(name=name, **kwargs)
            case _:
                kwargs["params"] = _SAMPLER_PARAMS[type](**kwargs.get("params", {}))
                column = SamplerColumn(name=name, type=type, **kwargs)
        return column

    def _get_next_dag_step(self, column_name: str) -> TaskConfig:
        column = self.get_column(column_name)
        if isinstance(column, LLMGenColumn):
            next_step = self._task_registry.GenerateColumnFromTemplate(
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
        """Retrieve the columns of a dataset given its file id.

        Args:
            file_id (str): File identifier for the dataset.

        Returns:
            list[str]: A list of column names present in the dataset.
        """
        retrieved_df = self._files.download_dataset(file_id)
        return retrieved_df.columns.tolist()

    @staticmethod
    def _get_last_evaluation_step_name(workflow_step_names: list[str]) -> str | None:
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
            "llm_gen_columns",
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


def _check_convert_to_json_str(
    column_names: list[str], *, indent: int | str | None = None
) -> list[str] | str | None:
    return (
        None
        if len(column_names) == 0
        else (
            column_names
            if len(column_names) < REPR_LIST_LENGTH_USE_JSON
            else json.dumps(column_names, indent=indent)
        )
    )
