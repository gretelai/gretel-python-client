from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Iterator, Optional, Type, TypeVar, Union

import pandas as pd

from gretel_client.config import ClientConfig
from gretel_client.projects import Project


def get_client(adapter: Union[Type[ClientAdapter], ClientAdapter]) -> Client:
    if not isinstance(adapter, ClientAdapter):
        adapter = adapter()
    return Client(adapter)


@dataclass
class WorkflowInterruption:
    message: str


@dataclass
class SubmitBatchWorkflowResponse:
    project: Project
    workflow_id: str
    workflow_run_id: str


class Client:

    _adapter: ClientAdapter

    def __init__(self, adapter: ClientAdapter):
        self._adapter = adapter

    @property
    def client_session(self) -> Optional[ClientConfig]:
        return self._adapter.client_session

    def run_task(
        self,
        name: str,
        config: dict,
        inputs: Optional[list[TaskInput]] = None,
        globals: Optional[dict] = None,
        verbose: bool = False,
    ) -> TaskOutput:
        if inputs is None:
            inputs = []
        if globals is None:
            globals = {}
        return self._adapter.run_task(name, config, inputs, globals, verbose)

    def get_workflow_preview(self, workflow_config: dict) -> Iterator:
        return self._adapter.stream_workflow_outputs(workflow_config)

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> SubmitBatchWorkflowResponse:
        return self._adapter.submit_batch_workflow(
            workflow_config, num_records, project_name, workflow_id
        )

    def get_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        format: Optional[str] = None,
    ) -> TaskOutput:
        return self._adapter.get_step_output(workflow_run_id, step_name, format)

    def download_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        output_dir: Path,
        format: Optional[str] = None,
    ) -> Path:
        return self._adapter.download_step_output(
            workflow_run_id, step_name, output_dir, format
        )

    def registry(self) -> list[dict]:
        return self._adapter.registry()


TaskInput = TypeVar("TaskInput")
TaskOutput = Union[pd.DataFrame, dict]


class ClientAdapter(ABC, Generic[TaskInput]):

    @abstractmethod
    def run_task(
        self,
        name: str,
        config: dict,
        inputs: list[TaskInput],
        globals: dict,
        verbose: bool = False,
    ) -> TaskOutput: ...

    @abstractmethod
    def stream_workflow_outputs(
        self, workflow: dict, verbose: bool = False
    ) -> Iterator[dict]: ...

    @abstractmethod
    def registry(self) -> list[dict]: ...

    @property
    def client_session(self) -> Optional[ClientConfig]:
        return None

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> SubmitBatchWorkflowResponse:
        raise NotImplementedError("Cannot submit batch Workflows")

    def get_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        format: Optional[str] = None,
    ) -> TaskOutput:
        raise NotImplementedError("Cannot get batch step outputs")

    def download_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        output_dir: Path,
        format: Optional[str] = None,
    ) -> Path:
        raise NotImplementedError("Cannot download batch artifacts")
