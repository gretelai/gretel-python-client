from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task


class SampleDataSeedsConfig(BaseModel):
    num_records: int = 10


class SampleDataSeeds(Task):
    """Sample data seed values for seeding synthetic data generation.

    Args:
        num_records: Number of records for the final dataset. If this task is
            being run in isolation for development purposes, must be <= 10 records.
        workflow_label: Label to append to the task name within a workflow. This can
            be helpful if you use the same task multiple times within a single workflow.
        client: Client object to use when running the task.
    """

    def __init__(
        self,
        num_records: int = 10,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=SampleDataSeedsConfig(num_records=num_records),
            workflow_label=workflow_label,
            client=client,
        )

    @property
    def name(self):
        return "sample_data_seeds"

    def run(self, categorical_data_seeds: dict) -> TaskOutput:
        if self.config.num_records > 10:
            raise ValueError("You can only preview up to to 10 records at a time.")
        return self._run(
            {"type": "categorical_data_seeds", "obj": categorical_data_seeds}
        )
