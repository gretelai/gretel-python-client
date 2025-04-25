import logging

from functools import wraps
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import yaml

from pydantic import BaseModel
from typing_extensions import Self

from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.safe_synthetics.blueprints import (
    load_blueprint_or_config,
    resolve_task_blueprint,
    TaskConfigError,
)
from gretel_client.workflows.builder import (
    task_to_step,
    WorkflowBuilder,
    WorkflowValidationError,
)
from gretel_client.workflows.configs.registry import Registry
from gretel_client.workflows.configs.tasks import EvaluateSafeSyntheticsDataset
from gretel_client.workflows.configs.workflows import Step
from gretel_client.workflows.tasks import TaskConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = logging.getLogger(__name__)


def handle_workflow_validation_error(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except WorkflowValidationError as e:
            logger.error(f"{str(e)}: task: {e.task_name!r} step: {e.step_name!r}:")
            for violation in e.field_violations:
                logger.error(f"\t{violation}")
            raise e

    return wrapper


class SafeSyntheticModelRegistry:

    def __init__(self, registry: Registry) -> None:
        self._model_mappings = {
            "tabular_ft": registry.TabularFt,
            "tabular_gan": registry.TabularGan,
            "text_ft": registry.TextFt,
            # todo: add me when tabdp is available on v2 workflows
            # "tabular_dp": None,
        }

    def get_config(self, model: str) -> type[TaskConfig]:
        if model in self._model_mappings:
            return self._model_mappings[model]
        raise TaskConfigError(
            f"{model!r} is not a "
            f"valid model identifier. Must be one of "
            f"{', '.join(self.models)}"
        )

    @property
    def models(self) -> list[str]:
        return list(self._model_mappings.keys())


class SafeSyntheticDataset:
    """
    A class for configuring and creating synthetic data generation workflows.
    """

    def __init__(
        self,
        builder: WorkflowBuilder,
        registry: Registry,
    ):
        self._builder = builder
        self._registry = registry
        self._synthetic_model_registry = SafeSyntheticModelRegistry(registry)

        # steps with fixed positions
        self._holdout = None
        self._custom_holdout_id = None
        self._evaluate_config = EvaluateSafeSyntheticsDataset()
        self._output_task = None

        # these steps are ordered
        self._tasks = []

    def transform(self, config: str | dict = "transform/default") -> Self:
        """
        Add a data transformation step to the workflow.

        Args:
            config: Transform configuration, either as a string path to a blueprint
                   or a dictionary of configuration options. Defaults to
                   "transform/default".
        """
        logger.info("Configuring transform step")
        transform_config = self._registry.Transform(**load_blueprint_or_config(config))
        self._output_task = transform_config
        self._tasks.append(transform_config)
        return self

    def data_source(
        self, data_source: str | Path | pd.DataFrame, use_data_source_step: bool = True
    ) -> Self:
        """
        Configure the input data source for the workflow.

        Args:
            data_source: Input data as either a file path, Path object, or pandas DataFrame
            use_data_source_step: Whether to create a dedicated data source step in the
                workflow. Defaults to True.
        """
        logger.info(
            f"Configuring generator for data source: {self._format_data_source(data_source)}"
        )
        self._builder.with_data_source(
            data_source, use_data_source_step=use_data_source_step
        )
        return self

    def holdout(
        self,
        holdout: float | int | str | Path | pd.DataFrame,
        max_holdout: int | None = None,
        group_by: str | None = None,
    ) -> Self:
        """
        Configure a holdout dataset. This holdout will get used during
        evaluation.

        Args:
            holdout: If a numeric value, indicates the amount of data to holdout,
                either as a fraction (float) or absolute number of records (int).
                Alternatively can be a file path to, or pandas DataFrame of,
                pre-configured test holdout data.
            max_holdout: Maximum number of records to include in holdout set
            group_by: Column name to use for grouped holdout selection
        """
        if isinstance(holdout, (str, Path, pd.DataFrame)):
            logger.info(f"Configuring holdout: {self._format_data_source(holdout)}")
            self._holdout = self._registry.Holdout()
            self._custom_holdout_id = self._builder.prepare_data_source(holdout)
        else:
            logger.info(f"Configuring holdout: {holdout}")
            self._holdout = self._registry.Holdout(
                holdout=holdout, max_holdout=max_holdout, group_by=group_by
            )
        return self

    def synthesize(
        self,
        model_or_blueprint_or_task: str | BaseModel | None = "tabular_ft/default",
        config: dict | str | None = None,
        num_records: int | None = None,
    ):
        """
        Configure the synthetic data generation model.

        Args:
            model_or_blueprint_or_task: Model specification, either as a
                string identifier, blueprint path, or BaseModel instance
            config: Additional configuration options as dict or YAML string
            num_records: Number of synthetic records to generate

        Raises:
            TaskConfigError: If the model configuration cannot be determined
        """
        logger.info(
            f"Configuring synthetic data generation model: {model_or_blueprint_or_task}"
        )

        task_klass = None
        task_config = None

        if isinstance(model_or_blueprint_or_task, BaseModel):
            task_klass = model_or_blueprint_or_task.__class__
            task_config = model_or_blueprint_or_task.model_dump()

        # a config can be either a pydantic base class, a
        # dictionary with a model key, or a yaml config
        elif config:
            if isinstance(config, str):
                config = yaml.safe_load(config)
            # if it's a dictionary, use the model key to
            # lookup the concrete class
            if isinstance(config, dict):
                if not model_or_blueprint_or_task:
                    raise TaskConfigError("You must specify a model")
                task_klass = self._synthetic_model_registry.get_config(
                    model_or_blueprint_or_task
                )
                task_config = config

        # setting a config takes precedence over blueprint resolution
        elif model_or_blueprint_or_task:
            # it's possible a customer configures their model with a name
            # such as "tabular_ft". if no config is specified, we reconfigure
            # that name to resolve to a blueprint, eg "tabular_ft/default".
            if (
                model_or_blueprint_or_task in self._synthetic_model_registry.models
                and not config
            ):
                model_or_blueprint_or_task = f"{model_or_blueprint_or_task}/default"

            try:
                blueprint = resolve_task_blueprint(model_or_blueprint_or_task)
                task_klass = self._synthetic_model_registry.get_config(
                    blueprint["task"]["name"]
                )
                task_config = blueprint["task"]["config"]
            except TaskConfigError:
                ...

        if not task_klass:
            raise TaskConfigError("Could not determine task config for builder call")

        if not task_config:
            task_config = {}

        task_config_for_klass = task_klass(**task_config)

        if num_records:
            try:
                task_config_num_records_adjusted = task_config_for_klass.model_dump()
                # by convention, num_records should be configured
                # on the generate field.
                if task_config_num_records_adjusted.get("generate"):
                    task_config_num_records_adjusted["generate"][
                        "num_records"
                    ] = num_records
                else:
                    task_config_num_records_adjusted["generate"] = {
                        "num_records": num_records
                    }

                task_config_for_klass = task_klass(**task_config_num_records_adjusted)

            except KeyError:
                raise TaskConfigError(
                    "We tried configuring num_records, but "
                    "the configuration doesn't support it."
                )
        self._output_task = task_config_for_klass
        self._tasks.append(task_config_for_klass)
        return self

    def evaluate(
        self,
        config: dict | str | EvaluateSafeSyntheticsDataset | None = None,
        disable: bool = False,
    ) -> Self:
        """
        Configure the evaluation step for comparing synthetic to original data.

        Args:
            config: Evaluation configuration as dict, YAML string, or concrete
                config instance
            disable: If True, disable the evaluation step. Defaults to False.
        """
        if config:
            logger.info("Configuring evaluate step")
            # if config is a string, we can safely assume it's
            # supposed to be a yaml string
            if isinstance(config, str):
                config = yaml.safe_load(config)
            if isinstance(config, dict):
                self._evaluate_config = EvaluateSafeSyntheticsDataset(**config)
            else:
                self._evaluate_config = config
        if disable:
            logger.info("Disabling evaluate step")
            self._evaluate_config = None
        return self

    def preview(self) -> None:
        self._builder.preview()

    @handle_workflow_validation_error
    def create(
        self,
        new_workflow: bool = False,
        name: Optional[str] = None,
        run_name: Optional[str] = None,
        wait_until_done: bool = False,
    ) -> WorkflowRun:
        """
        Create and optionally execute the configured synthetic data generation
        pipeline.

        Args:
            new_workflow: If True, create a new workflow instead of using existing
            name: Name for the workflow
            run_name: Name for this specific workflow run
            wait_until_done: If True, wait for workflow completion before returning

        Returns:
            WorkflowRun instance representing the created workflow

        Raises:
            WorkflowValidationError: If the workflow configuration is invalid
        """
        # Ensures that a new workflow is created for the run
        if new_workflow:
            self._builder.for_workflow(None)

        # Holdout should always be the first step in our workflow
        holdout_step = None
        if self._holdout:
            holdout_step = task_to_step(self._holdout)
            self._builder.add_step(
                holdout_step,
                step_inputs=[self._builder.data_source, self._custom_holdout_id],
            )

        # Add all our ordered tasks
        self._builder.add_steps(self._tasks)

        # Evaluate is going to be the final step in the workflow.
        # We need to determine the reference and output
        # dataset from the previous steps in the workflow.
        if self._evaluate_config:
            evaluate_step = task_to_step(self._evaluate_config)

            # Reference data is either going to be the holdout step
            # if one exists, or the input file to the workflow.
            reference = None
            if holdout_step:
                reference = holdout_step
            elif self._builder.data_source:
                reference = self._builder.data_source

            # The output dataset is always going to be the last
            # step in a SSD based workflow
            output = self._output_task

            # For now, using evaluate in a SSD workflow requires both
            # a output and reference dataset. In the future, this might
            # change.
            if reference and output:
                self._builder.add_step(evaluate_step, step_inputs=[output, reference])
            else:
                logger.debug(
                    "Evaluate requires both reference and output datasets. No Evaluate step added."
                )

        return self._builder.run(
            name=name, run_name=run_name, wait_until_done=wait_until_done
        )

    def _format_data_source(self, data_source: str | Path | pd.DataFrame) -> str:
        if isinstance(data_source, pd.DataFrame):
            return f"DataFrame {data_source.shape}"
        else:
            return str(data_source)

    def _last_step_for_prefix(self, prefix: str, steps: list[Step]) -> Optional[Step]:
        found = None
        for step in steps:
            if step.name.startswith(prefix):
                found = step
        return found

    def builder(self) -> WorkflowBuilder:
        """Get the underlying WorkflowBuilder instance."""
        return self._builder


class SafeSyntheticDatasetFactory:

    def __init__(self, resource_provider: GretelResourceProviderProtocol) -> None:
        self._resource_provider = resource_provider

    def from_data_source(
        self,
        data_source: str | Path | pd.DataFrame,
        holdout: float | int | str | Path | pd.DataFrame | None = 0.05,
        max_holdout: int | None = 2000,
        group_by: str | None = None,
        use_data_source_step: bool = True,
    ) -> SafeSyntheticDataset:
        safe_synthetic_dataset = SafeSyntheticDataset(
            self._resource_provider.workflows.builder(),
            Registry(),
        )

        safe_synthetic_dataset.data_source(
            data_source, use_data_source_step=use_data_source_step
        )

        if holdout is not None:
            safe_synthetic_dataset.holdout(holdout, max_holdout, group_by)
        return safe_synthetic_dataset
