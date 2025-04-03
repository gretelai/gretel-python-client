import logging

from pathlib import Path
from typing import Optional, Union

import pandas as pd

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
from gretel_client.workflows.configs.tasks import EvaluateSsDataset
from gretel_client.workflows.configs.workflows import Step
from gretel_client.workflows.tasks import TaskConfig
from gretel_client.workflows.workflow import WorkflowRun

logger = logging.getLogger(__name__)


def handle_workflow_validation_error(func):

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
        self._evaluate_config = EvaluateSsDataset()
        self._synthesis_task = None

        # these steps are ordered
        self._tasks = []

    def transform(self, config: Union[str, dict] = "transform/default") -> Self:
        logger.info(f"Configuring transform step")
        self._tasks.append(self._registry.Transform(**load_blueprint_or_config(config)))
        return self

    def data_source(self, data_source: Union[str, Path, pd.DataFrame]) -> Self:
        logger.info(f"Configuring generator for data source: {data_source}")
        self._builder.with_data_source(data_source)
        return self

    def holdout(self, holdout: Union[float, int]) -> Self:
        logger.info(f"Configuring holdout: {holdout}")
        self._holdout = self._registry.Holdout(holdout=holdout)
        return self

    def synthesize(
        self,
        model_or_blueprint_or_task: Optional[
            Union[str, BaseModel]
        ] = "tabular_ft/default",
        config: Optional[dict] = None,
        num_records: Optional[int] = None,
    ):
        logger.info(
            f"Configuring synthetic data generation model: {model_or_blueprint_or_task}"
        )

        task_klass = None
        task_config = None

        if isinstance(model_or_blueprint_or_task, BaseModel):
            task_klass = model_or_blueprint_or_task.__class__
            task_config = model_or_blueprint_or_task.model_dump()

        # a config can be either a pydantic base class, or
        # dictionary with a model key
        elif config:
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
        self._synthesis_task = task_config_for_klass
        self._tasks.append(task_config_for_klass)
        return self

    def evaluate(
        self,
        config: Optional[Union[dict, EvaluateSsDataset]] = None,
        disable: bool = False,
    ) -> Self:
        if config:
            logger.info("Configuring evaluate step")
            if isinstance(config, dict):
                self._evaluate_config = EvaluateSsDataset(**config)
            else:
                self._evaluate_config = config
        if disable:
            logger.info("Disabling evaluate step")
            self._evaluate_config = None
        return self

    def preview(self) -> None:
        self._builder.preview()

    @handle_workflow_validation_error
    def create(self, new_workflow: bool = False, wait: bool = True) -> WorkflowRun:
        # Ensures that a new workflow is created for the run
        if new_workflow:
            self._builder.for_workflow(None)

        # Holdout should always be the first step in our workflow
        holdout_step = None
        if self._holdout:
            holdout_step = task_to_step(self._holdout)
            self._builder.add_step(holdout_step)

        # Add all our ordered tasks
        self._builder.add_steps(self._tasks)

        # Evaluate is going to be the final step in the workflow.
        # We need to determine the training and synthetic
        # dataset from the previous steps in the workflow.
        if self._evaluate_config:
            evaluate_step = task_to_step(self._evaluate_config)

            # Training data is either going to be the holdout step
            # if one exists, or the input file to the workflow.
            train = None
            if holdout_step:
                train = holdout_step
            elif self._builder.data_source:
                train = self._builder.data_source

            # The training dataset is always going to be the last
            # step in a SSD based workflow
            synth = self._synthesis_task

            # For now, using evaluate in a SSD workflow requires both
            # a synthetic and training dataset. In the future, this might
            # change.
            if train and synth:
                self._builder.add_step(evaluate_step, step_inputs=[synth, train])
            else:
                logger.debug(
                    "Evaluate requires both training and synthetic datasets. No Evaluate step added."
                )

        return self._builder.run(wait=wait)

    def _last_step_for_prefix(self, prefix: str, steps: list[Step]) -> Optional[Step]:
        found = None
        for step in steps:
            if step.name.startswith(prefix):
                found = step
        return found

    def builder(self) -> WorkflowBuilder:
        return self._builder


class SafeSyntheticDatasetFactory:

    def __init__(self, resource_provider: GretelResourceProviderProtocol) -> None:
        self._resource_provider = resource_provider

    def from_data_source(
        self,
        data_source,
        holdout: Optional[Union[float, int]] = 0.05,
    ) -> SafeSyntheticDataset:
        safe_synthetic_dataset = SafeSyntheticDataset(
            self._resource_provider.workflows.builder(),
            Registry(),
        )

        safe_synthetic_dataset.data_source(data_source)

        if holdout:
            safe_synthetic_dataset.holdout(holdout)
        return safe_synthetic_dataset
