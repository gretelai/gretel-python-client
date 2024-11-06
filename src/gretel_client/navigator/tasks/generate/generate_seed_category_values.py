from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.gretel.config_setup import smart_load_yaml
from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.types import (
    DEFAULT_MODEL_SUITE,
    ModelSuite,
    SeedCategory,
)


class GenerateSeedCategoryValuesConfig(BaseModel):
    seed_categories: list[SeedCategory]
    dataset_context: str = ""


class GenerateSeedCategoryValues(Task):
    """Generate values for seed categories.

    Args:
        seed_categories: List of `SeedCategory` objects or dicts representing seed categories.
        dataset_context: Context for the dataset. This is used to provide additional context
            to the model when generating values.
        workflow_label: Label to append to the task name within a workflow. This can
            be helpful if you use the same task multiple times within a single workflow.
        client: Client object to use when running the task.
        model_suite: Suite of models to use. Must be a member of the ModelSuite enum.
    """

    def __init__(
        self,
        seed_categories: Union[str, Path, list[dict], list[SeedCategory]],
        dataset_context: Optional[str] = None,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        super().__init__(
            config=GenerateSeedCategoryValuesConfig(
                seed_categories=self._check_and_get_seed_categories(seed_categories),
                dataset_context=dataset_context or "",
            ),
            workflow_label=workflow_label,
            client=client,
            model_suite=model_suite,
        )

    @staticmethod
    def _check_and_get_seed_categories(
        categories: Union[str, Path, list[dict], list[SeedCategory]]
    ) -> list[SeedCategory]:
        if isinstance(categories, (str, Path)):
            categories = smart_load_yaml(categories).get("seed_categories")

        if not isinstance(categories, list):
            raise ValueError(
                "`seed_categories` must be a list of dicts or SeedCategory objects"
            )

        # Convert dicts to DataSeedColumn objects to ensure they are valid.
        if all(isinstance(seed, dict) for seed in categories):
            categories = SeedCategory.from_dicts(categories)

        if not all(isinstance(seed, SeedCategory) for seed in categories):
            raise ValueError(
                "`seed_categories` must be a list of dicts or SeedCategory objects"
            )

        return categories

    @property
    def name(self) -> str:
        return "generate_seed_category_values"

    def run(self) -> TaskOutput:
        return self._run()
