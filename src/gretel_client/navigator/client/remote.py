from __future__ import annotations

import json
import logging

from dataclasses import dataclass
from typing import Iterator, Optional, Union

import pandas as pd
import requests

from pydantic import BaseModel

from gretel_client import Gretel
from gretel_client.config import get_session_config
from gretel_client.navigator.client.interface import (
    ClientAdapter,
    StructuredInput,
    TaskInput,
    TaskOutput,
)
from gretel_client.navigator.log import get_logger

DATA_FRAME_OUTPUT_TYPE = "data_frame"
STREAMING_RECORD_OUTPUT_TYPE = "streaming_record"
LOG_OUTPUT_TYPE = "log_line"
STEP_STATE_CHANGE_TYPE = "step_state_change"

NON_ATTRIBUTE_OUTPUT_TYPES = {
    DATA_FRAME_OUTPUT_TYPE,
    STREAMING_RECORD_OUTPUT_TYPE,
    LOG_OUTPUT_TYPE,
    STEP_STATE_CHANGE_TYPE,
}

gretel_interface_logger = logging.getLogger("gretel_client.gretel.interface")
gretel_interface_logger.setLevel(logging.WARNING)

logger = get_logger(__name__, level="INFO")


@dataclass
class AttributeOutput:
    name: str
    data: object


class StepOutput(BaseModel):
    step: str
    type: str
    # - list is used for data_frame outputs
    # - dict is used for streaming_record and attribute outputs
    output: Union[list, dict]

    def is_data_frame(self) -> bool:
        return self.type == DATA_FRAME_OUTPUT_TYPE

    def is_attribute(self) -> bool:
        return self.type not in NON_ATTRIBUTE_OUTPUT_TYPES

    def is_streaming_record(self) -> bool:
        return self.type == STREAMING_RECORD_OUTPUT_TYPE

    def is_log(self) -> bool:
        return self.type == LOG_OUTPUT_TYPE

    def is_step_state_change(self) -> bool:
        return self.type == LOG_OUTPUT_TYPE


class RemoteTaskOutput(TaskOutput):

    def __init__(self, response: requests.Response):
        super().__init__()

        self._response = response
        self._data_outputs: list[pd.DataFrame] = []
        self._attributes: list[AttributeOutput] = []

    def _consume_single_output(self, record: dict) -> StepOutput:
        step_output = StepOutput.model_validate(record)

        # Collect only the data outputs and attributes, the rest is passed through.
        if step_output.is_data_frame():
            self._data_outputs.append(pd.DataFrame.from_records(step_output.output))
        elif step_output.is_attribute():
            self._attributes.append(
                AttributeOutput(name=step_output.type, data=step_output.output)
            )

        return step_output

    def data_outputs(self) -> list[pd.DataFrame]:
        self._ensure_consumed()
        return self._data_outputs

    def attribute_outputs(self) -> list[dict]:
        self._ensure_consumed()
        return [{attr.name: attr.data} for attr in self._attributes]

    def as_input(self) -> list[TaskInput]:
        inputs = []
        for output in self.data_outputs():
            inputs.append(
                TaskInput(
                    structured_data=StructuredInput(
                        dataset=output.to_dict(orient="records")
                    ),
                )
            )

        attributes = [{output.name: output.data} for output in self._attributes]
        if attributes:
            inputs.append(
                TaskInput(structured_data=StructuredInput(attributes=attributes))
            )

        return inputs

    def __iter__(self) -> Iterator:
        for json_str in self._response.iter_lines(decode_unicode=True):
            try:
                yield self._consume_single_output(json.loads(json_str))
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON record: {json_str!r}")

        self._consumed = True
        self._response.close()


class RemoteClient(ClientAdapter):

    def __init__(self, jarvis_endpoint: str = "https://jarvis.dev.gretel.cloud"):
        self._session = get_session_config()
        self._req_headers = {"Authorization": self._session.api_key}
        self._jarvis_endpoint = jarvis_endpoint

    def run_task(self, name: str, config: dict, inputs: list[TaskInput]) -> TaskOutput:
        if config is None:
            config = {}
        if inputs is None:
            inputs = []

        inputs_as_json = []
        for _input in inputs:
            if _input.raw_data:
                raise NotImplementedError(
                    "RemoteClient doesn't support raw data inputs."
                )
            inputs_as_json.append(_input.structured_data.serialize())

        response = requests.post(
            f"{self._jarvis_endpoint}/tasks/exec",
            json={"name": name, "config": config, "inputs": inputs_as_json},
            headers=self._req_headers,
            stream=True,
        )
        response.raise_for_status()

        return RemoteTaskOutput(response)

    def stream_workflow_outputs(self, workflow: dict) -> Iterator:
        with requests.post(
            f"{self._jarvis_endpoint}/workflows/exec_streaming",
            json=workflow,
            headers=self._req_headers,
            stream=True,
        ) as outputs:
            outputs.raise_for_status()

            for output in outputs.iter_lines():
                yield json.loads(output.decode("utf-8"))

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
    ) -> dict:

        for step in workflow_config["steps"]:
            if "num_records" in step["config"]:
                step["config"]["num_records"] = num_records

        gretel = Gretel(session=self._session)
        gretel.set_project(name=project_name)
        project = gretel.get_project()

        logger.info(
            f"🔗 Connecting to your [link={project.get_console_url()}]Gretel Project[/link]",
            extra={"markup": True},
        )
        logger.info(f"🚀 Submitting batch workflow to generate {num_records} records")

        response = requests.post(
            f"{self._jarvis_endpoint}/workflows/exec_batch",
            json={
                "workflow_config": workflow_config,
                "project_id": project.project_guid,
            },
            headers=self._req_headers,
        )
        response.raise_for_status()
        workflow_ids = response.json()
        workflow_run_url = (
            f"{project.get_console_url().replace(project.project_guid, '')}workflows/"
            f"{workflow_ids['workflow_id']}/runs/{workflow_ids['workflow_run_id']}"
        )

        logger.info(
            f"👀 Follow along: [link={workflow_run_url}]Workflow Run[/link]",
            extra={"markup": True},
        )

    def registry(self) -> list[dict]:
        response = requests.get(
            f"{self._jarvis_endpoint}/registry", headers=self._req_headers
        )
        response.raise_for_status()

        return response.json()["tasks"]
