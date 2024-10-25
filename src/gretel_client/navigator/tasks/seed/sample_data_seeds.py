from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task


class SampleDataSeedsConfig(BaseModel):
    num_records: int = 10


class SampleDataSeeds(Task):

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

    def run(self, data_seed_columns: dict) -> TaskOutput:
        if self.config.num_records > 10:
            raise ValueError("You can only preview up to to 10 records at a time.")
        return self._run({"type": "data_seed_columns", "obj": data_seed_columns})
