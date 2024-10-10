from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.tasks.base import Task, TaskResults


class SeedFromRecordsConfig(BaseModel):
    records: list[dict]


class SeedFromRecords(Task):

    def __init__(self, records: list[dict], workflow_label: Optional[str] = None):
        super().__init__(
            config=SeedFromRecordsConfig(records=records), workflow_label=workflow_label
        )

    @property
    def name(self) -> str:
        return "seed_from_records"

    def run(self) -> TaskResults:
        return self._run(attributes=[{"records": self.config.records}])
