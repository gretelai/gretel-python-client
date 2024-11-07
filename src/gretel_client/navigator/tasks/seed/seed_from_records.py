from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task


class SeedFromRecordsConfig(BaseModel):
    records: list[dict]


class SeedFromRecords(Task):

    def __init__(
        self,
        records: list[dict],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        super().__init__(
            config=SeedFromRecordsConfig(records=records),
            workflow_label=workflow_label,
            client=client,
        )

    @property
    def name(self) -> str:
        return "seed_from_records"

    def run(self) -> TaskOutput:
        return self._run(self.config.records)
