from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator, Optional, Type, Union

import pandas as pd

from pydantic import BaseModel, Field


def get_client(adapter: Union[Type[ClientAdapter], ClientAdapter]) -> Client:
    if not isinstance(adapter, ClientAdapter):
        adapter = adapter()
    return Client(adapter)


class Client:

    _adapter: ClientAdapter

    def __init__(self, adapter: ClientAdapter):
        self._adapter = adapter

    def run_task(
        self, name: str, config: dict, inputs: list[TaskInput] = None
    ) -> TaskOutput:
        if inputs is None:
            inputs = []
        return self._adapter.run_task(name, config, inputs)

    def get_workflow_preview(self, workflow_config: dict) -> Iterator:
        return self._adapter.stream_workflow_outputs(workflow_config)

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
    ):
        return self._adapter.submit_batch_workflow(
            workflow_config, num_records, project_name
        )

    def registry(self) -> list[dict]:
        return self._adapter.registry()


class ClientAdapter(ABC):

    @abstractmethod
    def run_task(
        self, name: str, config: dict, inputs: list[TaskInput]
    ) -> TaskOutput: ...

    @abstractmethod
    def stream_workflow_outputs(self, workflow: dict) -> Iterator: ...

    @abstractmethod
    def registry(self) -> list[dict]: ...

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
    ):
        raise NotImplementedError("Cannot submit batch Workflows")


class TaskOutput(ABC):
    """
    Abstract TaskOutput class that represents the output of a task.
    Task output (regardless of the client) is always a stream, so one way of consuming
    this output is to iterate over it (`__iter__()`).

    Additionally, when the output is consumed, data outputs and attributes are captured
    and can be retrieved with `data_outputs()` and `attribute_outputs()` methods.

    Note: if the stream wasn't consumed yet, calling these methods will consume the stream.
    """

    def __init__(self):
        self._consumed = False

    def _ensure_consumed(self) -> None:
        if not self._consumed:
            self._consume()

        self._consumed = True

    def _consume(self) -> None:
        if self._consumed:
            return

        # exhaust the iterator, without doing anything with the records
        for _ in self:
            pass

    @abstractmethod
    def as_input(self) -> list[TaskInput]:
        """
        Converts this output to inputs that can be passed to other tasks.
        """
        ...

    @abstractmethod
    def data_outputs(self) -> list[pd.DataFrame]: ...

    @abstractmethod
    def attribute_outputs(self) -> list[dict]: ...

    @abstractmethod
    def __iter__(self) -> Iterator: ...


class StructuredInput(BaseModel):
    dataset: Optional[list[dict]] = None
    attributes: list[dict] = Field(default_factory=list)

    def serialize(self) -> dict:
        return self.model_dump(exclude_none=True)


class TaskInput(BaseModel):
    raw_data: Optional[bytes] = None
    structured_data: Optional[StructuredInput] = None

    @classmethod
    def from_dataset(cls, dataset: pd.DataFrame) -> TaskInput:
        return cls(
            structured_data=StructuredInput(dataset=dataset.to_dict(orient="records"))
        )

    @classmethod
    def from_attribute(cls, name: str, value: object) -> TaskInput:
        return cls(structured_data=StructuredInput(attributes=[{name: value}]))
