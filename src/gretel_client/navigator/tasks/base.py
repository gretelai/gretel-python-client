from abc import ABC, abstractmethod
from typing import Optional, Union

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, ClientAdapter, TaskOutput
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.tasks.io import Dataset
from gretel_client.navigator.tasks.types import (
    check_model_suite,
    DEFAULT_MODEL_SUITE,
    ModelSuite,
    RecordsT,
)


class Task(ABC):

    def __init__(
        self,
        config: BaseModel,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
    ):
        self.config = config
        self.workflow_label = workflow_label
        self._client = client or get_navigator_client()
        self._globals = {"model_suite": check_model_suite(model_suite)}

    def _records_to_dataset_if_needed(
        self, dataset: Union[Dataset, RecordsT]
    ) -> Dataset:
        if isinstance(dataset, Dataset):
            return dataset
        return Dataset.from_records(dataset)

    def _set_client(self, adapter: ClientAdapter):
        """Set client adapter for task execution.

        This is an internal method that is not useable by end users.
        """
        self._client = get_navigator_client(adapter)

    def _run(self, *inputs) -> TaskOutput:
        return self._client.run_task(
            name=self.name,
            config=self.config.model_dump(),
            inputs=list(inputs),
            globals=self._globals,
        )

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run(self, *args, **kwargs) -> TaskOutput: ...
