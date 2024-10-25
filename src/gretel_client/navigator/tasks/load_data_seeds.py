from typing import Optional, Union

from gretel_client.navigator.client.interface import Client, TaskOutput
from gretel_client.navigator.tasks.base import Task
from gretel_client.navigator.tasks.types import DataSeedColumns


class LoadDataSeeds(Task):

    def __init__(
        self,
        seed_columns: Union[dict, DataSeedColumns],
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        if seed_columns and isinstance(seed_columns, dict):
            seed_columns = DataSeedColumns(**seed_columns)
        super().__init__(
            config=seed_columns, workflow_label=workflow_label, client=client
        )

    @property
    def name(self) -> str:
        return "load_data_seeds"

    def run(self) -> TaskOutput:
        return self._run()
