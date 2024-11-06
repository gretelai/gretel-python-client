from __future__ import annotations

import json
import logging

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Iterator, Optional, Union

import pandas as pd
import pydantic
import requests
import smart_open

from inflection import underscore
from requests import HTTPError
from rich import print as rich_print

from gretel_client.config import ClientConfig
from gretel_client.gretel.interface import Gretel
from gretel_client.navigator.client.interface import (
    ClientAdapter,
    SubmitBatchWorkflowResponse,
    TaskInput,
    TaskOutput,
)
from gretel_client.navigator.log import get_logger

gretel_interface_logger = logging.getLogger("gretel_client.gretel.interface")
gretel_interface_logger.setLevel(logging.WARNING)

logger = get_logger(__name__, level=logging.INFO)


Serializable = Union[pydantic.BaseModel, pd.DataFrame, dict]


@dataclass
class Message:

    step: str
    """The name of the step"""

    stream: str
    """
    The stream the message should be associated with.

    We use multiple streams so that we can differentiate between different types of outputs.
    """

    payload: dict
    """The actual value of the output"""

    type: str
    """The type of message"""

    ts: datetime
    """The date and time the message was created"""

    @classmethod
    def from_dict(cls, message: dict) -> Message:
        message["ts"] = datetime.fromisoformat(message["ts"])
        return cls(**message)


def workflow_preview(workflow_outputs: Iterator, verbose: bool = False) -> TaskOutput:
    terminal_output = None
    for message_dict in workflow_outputs:
        message = Message.from_dict(message_dict)
        if message.stream == "step_outputs":
            if message.type == "dataset":
                terminal_output = pd.DataFrame.from_records(
                    message.payload.get("dataset")
                )
            else:
                terminal_output = message.payload
    return terminal_output


class RemoteClient(ClientAdapter[Serializable]):

    def __init__(self, api_endpoint: str, client_session: ClientConfig):
        self._session = client_session
        self._req_headers = {"Authorization": self._session.api_key}
        self._api_endpoint = api_endpoint

        logger.debug(f"🌎 Connecting to {self._api_endpoint}")

    # todo: pass an event handler and log non task outputs
    def run_task(
        self,
        name: str,
        config: dict,
        inputs: list[TaskInput],
        globals: dict,
        verbose: bool = False,
    ) -> TaskOutput:
        if config is None:
            config = {}
        if inputs is None:
            inputs = []
        if globals is None:
            globals = {}

        inputs = serialize_inputs(inputs)

        response = requests.post(
            f"{self._api_endpoint}/v2/workflows/tasks/exec",
            json={"name": name, "config": config, "inputs": inputs, "globals": globals},
            headers=self._req_headers,
            stream=True,
        )

        try:
            response.raise_for_status()
        except HTTPError as e:
            rich_print(f"Got error: {str(e)}")
            rich_print(response.json())
            raise e

        with response as messages:
            try:
                for o in messages.iter_lines():
                    message = json.loads(o)
                    if message["stream"] == "step_outputs":
                        if message["type"] == "dataset":
                            return pd.DataFrame.from_records(
                                message["payload"]["dataset"]
                            )
                        return message["payload"]
            except Exception as e:
                rich_print(e)

        raise Exception("Did not receive output for task")

    def stream_workflow_outputs(
        self, workflow: dict, verbose: bool = False
    ) -> Iterator[Message]:
        with requests.post(
            f"{self._api_endpoint}/v2/workflows/exec_streaming",
            json=workflow,
            headers=self._req_headers,
            stream=True,
        ) as outputs:
            outputs.raise_for_status()

            for output in outputs.iter_lines():
                yield Message.from_dict(json.loads(output))

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
    ) -> SubmitBatchWorkflowResponse:

        for step in workflow_config["steps"]:
            if "num_records" in step["config"]:
                step["config"]["num_records"] = num_records

        gretel = Gretel(session=self._session)
        gretel.set_project(name=project_name)
        project = gretel.get_project()

        logger.info("🛜 Connecting to your Gretel Project:")
        logger.info(f"🔗 -> {project.get_console_url()}")

        response = requests.post(
            f"{self._api_endpoint}/v2/workflows/exec_batch",
            json={
                "workflow_config": workflow_config,
                "project_id": project.project_guid,
            },
            headers=self._req_headers,
        )
        response.raise_for_status()
        response_body = response.json()
        batch_response = SubmitBatchWorkflowResponse(
            project=project,
            workflow_id=response_body["workflow_id"],
            workflow_run_id=response_body["workflow_run_id"],
        )
        workflow_run_url = (
            f"{project.get_console_url().replace(project.project_guid, '')}workflows/"
            f"{batch_response.workflow_id}/runs/{batch_response.workflow_run_id}"
        )

        logger.info(f"▶️ Starting your workflow run to generate {num_records} records:")
        logger.info(f"  |-- project_name: {project.name}")
        logger.info(f"  |-- project_id: {project.project_guid}")
        logger.info(f"  |-- workflow_id: {batch_response.workflow_id}")
        logger.info(f"  |-- workflow_run_id: {batch_response.workflow_run_id}")
        logger.info(f"🔗 -> {workflow_run_url}")

        return batch_response

    def get_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        format: Optional[str] = None,
    ) -> TaskOutput:
        with self._request_artifact(workflow_run_id, step_name, format) as response:
            content_type = response.headers.get("content-type")
            if content_type == "application/json":
                return json.load(BytesIO(response.content))
            elif content_type == "application/vnd.apache.parquet":
                return pd.read_parquet(BytesIO(response.content))
            else:
                raise Exception(
                    f"Cannot get output format {format!r} as TaskOutput. Try downloading instead."
                )

    def download_step_output(
        self,
        workflow_run_id: str,
        step_name: str,
        output_dir: Path,
        format: Optional[str] = None,
    ) -> Path:
        with self._request_artifact(workflow_run_id, step_name, format) as response:
            filename = response.headers.get("content-disposition").split("filename=")[1]
            out_file = output_dir / filename
            with smart_open.open(out_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return out_file

    @contextmanager
    def _request_artifact(
        self,
        workflow_run_id: str,
        step_name: str,
        format: Optional[str] = None,
    ) -> Iterator[requests.models.Response]:
        endpoint = f"{self._api_endpoint}/v2/workflows/runs/{workflow_run_id}/{step_name}/outputs"
        params = {"format": format}
        with requests.get(
            endpoint,
            headers=self._req_headers,
            params=params,
            stream=True,
        ) as response:
            response.raise_for_status()
            yield response

    def registry(self) -> list[dict]:
        response = requests.get(
            f"{self._api_endpoint}/v2/workflows/registry", headers=self._req_headers
        )
        response.raise_for_status()

        return response.json()["tasks"]


def serialize_inputs(inputs: list[TaskInput]) -> list[dict]:
    inputs_as_json = []
    for _input in inputs:
        if isinstance(_input, dict):
            inputs_as_json.append(_input)
        if isinstance(_input, pydantic.BaseModel):
            inputs_as_json.append(_serialize_pydantic(_input))
        if isinstance(_input, pd.DataFrame):
            inputs_as_json.append(_serialize_df(_input))
    return inputs_as_json


def _serialize_df(df: pd.DataFrame) -> dict:
    return {"type": "dataset", "obj": {"dataset": df.to_dict(orient="records")}}


def _serialize_pydantic(pydantic_model: pydantic.BaseModel) -> dict:
    return {
        "type": underscore(pydantic_model.__name__),
        "obj": pydantic_model.model_dump(),
    }
