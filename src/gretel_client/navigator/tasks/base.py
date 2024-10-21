from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

import pandas as pd

from pydantic import BaseModel

from gretel_client.navigator.client.interface import (
    ClientAdapter,
    StructuredInput,
    TaskInput,
)
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.tasks.io import Dataset


@dataclass
class TaskResults:
    dataset: Optional[Dataset] = None
    attributes: Optional[list[dict]] = None


class Task(ABC):

    def __init__(self, config: BaseModel, workflow_label: Optional[str] = None):
        self.config = config
        self.workflow_label = workflow_label
        self._client = get_navigator_client()

    @staticmethod
    def _create_task_inputs(
        dataset: Optional[Dataset] = None, attributes: Optional[list[dict]] = None
    ) -> list[TaskInput]:
        if dataset is None and attributes is None:
            return []
        structured_data = StructuredInput(
            dataset=(
                None
                if dataset is None
                else (
                    dataset.to_dict(orient="records")
                    if isinstance(dataset, Dataset)
                    else dataset
                )
            ),
            attributes=attributes or [],
        )
        return [TaskInput(structured_data=structured_data)]

    def _set_client(self, adapter: ClientAdapter):
        """Set client adapter for task execution.

        This is an internal method that is not useable by end users.
        """
        self._client = get_navigator_client(adapter)

    def _run(
        self,
        dataset: Optional[Union[Dataset, list[dict]]] = None,
        attributes: Optional[list[dict]] = None,
    ) -> TaskResults:
        output = self._client.run_task(
            name=self.name,
            config=self.config.model_dump(),
            inputs=self._create_task_inputs(dataset, attributes),
        )
        return TaskResults(
            dataset=(
                pd.concat(out, axis=0, ignore_index=True)
                if (out := output.data_outputs())
                else None
            ),
            attributes=output.attribute_outputs(),
        )

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run(self) -> TaskResults: ...
