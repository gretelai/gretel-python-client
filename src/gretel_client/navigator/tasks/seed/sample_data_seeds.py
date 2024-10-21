from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.tasks.base import Task, TaskResults


class SampleDataSeedsConfig(BaseModel):
    num_records: int = 10


class SampleDataSeeds(Task):

    def __init__(self, num_records: int = 10, workflow_label: Optional[str] = None):
        super().__init__(
            config=SampleDataSeedsConfig(num_records=num_records),
            workflow_label=workflow_label,
        )

    @property
    def name(self):
        return "sample_data_seeds"

    def run(self, data_seed_columns: list[dict]) -> TaskResults:
        if self.config.num_records > 10:
            raise ValueError("You can only preview up to to 10 records at a time.")
        return self._run(attributes=data_seed_columns)
