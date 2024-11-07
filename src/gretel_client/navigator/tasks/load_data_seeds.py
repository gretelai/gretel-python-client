from typing import Optional, Union

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.types import CategoricalDataSeeds


class LoadDataSeeds(Task):
    """Load categorical data seeds into the Navigator Workflow.

    Args:
        categorical_data_seeds: A `CategoricalDataSeeds` object or a dictionary that
            can be used to create one.
        workflow_label: Label to append to the task name within a workflow. This can
            be helpful if you use the same task multiple times within a single workflow.
        client: Client object to use when running the task.
    """

    def __init__(
        self,
        categorical_data_seeds: Union[dict, CategoricalDataSeeds],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        if categorical_data_seeds and isinstance(categorical_data_seeds, dict):
            categorical_data_seeds = CategoricalDataSeeds(**categorical_data_seeds)
        super().__init__(
            config=categorical_data_seeds, workflow_label=workflow_label, client=client
        )

    @property
    def name(self) -> str:
        return "load_data_seeds"

    def run(self) -> TaskOutput:
        return self._run()
