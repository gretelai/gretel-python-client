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
from requests.exceptions import ChunkedEncodingError

from gretel_client.config import ClientConfig
from gretel_client.gretel.interface import Gretel
from gretel_client.navigator.client.interface import (
    ClientAdapter,
    SubmitBatchWorkflowResponse,
    TaskInput,
    TaskOutput,
    WorkflowInterruption,
)
from gretel_client.navigator.log import get_logger

gretel_interface_logger = logging.getLogger("gretel_client.gretel.interface")
gretel_interface_logger.setLevel(logging.WARNING)

logger = get_logger(__name__, level=logging.INFO)


Serializable = Union[pydantic.BaseModel, pd.DataFrame, dict]

BROKEN_RESPONSE_STREAM_ERROR_MESSAGE = (
    "Error consuming API response stream. "
    "This error is likely temporary and we recommend retrying your request. "
    "If this problem persists please contact support."
)


class NavigatorApiError(Exception):
    """
    Base error type for all errors related to
    communicating with the API.
    """


class NavigatorApiClientError(NavigatorApiError):
    """
    Error type for 4xx error responses from the API.
    """


class NavigatorApiServerError(NavigatorApiError):
    """
    Error type for 5xx error responses from the API.
    """


class NavigatorApiStreamingResponseError(NavigatorApiError):
    """
    Error type for issues encountered while handling a
    streaming response from the API, such as it being
    incomplete or malformed.
    """


class WorkflowTaskError(Exception):
    """
    Represents an error returned by the Task. This error
    is most likely related to an issue with the Task
    itself. If you see this error check your Task config
    first. If the issue persists, the error might be a bug
    in the remote Task implementation.
    """


@dataclass
class Message:

    step: Optional[str]
    """
    The name of the step.
    
    If the message is not associated with a step, this value will be `None`.
    """

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
    def from_dict(cls, message: dict, raise_on_error: bool = False) -> Message:
        message["ts"] = datetime.fromisoformat(message["ts"])

        if "step" not in message:
            message["step"] = None
        deserialized_message = cls(**message)

        if raise_on_error:
            _raise_on_task_error(deserialized_message)

        return deserialized_message


def workflow_preview(workflow_outputs: Iterator, verbose: bool = False) -> TaskOutput:
    terminal_output = None
    for message_dict in workflow_outputs:
        message = Message.from_dict(message_dict, raise_on_error=True)
        if message.stream == "step_outputs":
            if message.type == "dataset":
                terminal_output = pd.DataFrame.from_records(
                    message.payload.get("dataset")
                )
            else:
                terminal_output = message.payload
    return terminal_output


def _raise_on_task_error(message: Message):
    """
    Inspects a message for messages known to contain
    fatal errors, and raises an exception from those
    messages.
    """
    if (
        message.type == "step_state_change"
        and message.payload.get("state", "") == "error"
    ):
        raise WorkflowTaskError(
            f"Step {message.step!r} failed: {message.payload.get('msg')}. Please check your Workflow config. "
            "If the issue persists please contact support."
        )


class RemoteClient(ClientAdapter[Serializable]):

    def __init__(self, api_endpoint: str, client_session: ClientConfig):
        self._session = client_session
        self._req_headers = {"Authorization": self._session.api_key}
        self._api_endpoint = api_endpoint

        logger.debug(f"🌎 Connecting to {self._api_endpoint}")

    @property
    def client_session(self) -> Optional[ClientConfig]:
        return self._session

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

        with requests.post(
            f"{self._api_endpoint}/v2/workflows/tasks/exec",
            json={"name": name, "config": config, "inputs": inputs, "globals": globals},
            headers=self._req_headers,
            stream=True,
        ) as response:
            _check_for_error_response(response)

            try:
                for output in response.iter_lines():
                    message = Message.from_dict(json.loads(output), raise_on_error=True)
                    if message.stream == "step_outputs":
                        if message.type == "dataset":
                            return pd.DataFrame.from_records(message.payload["dataset"])
                        return message.payload
            except ChunkedEncodingError:
                raise NavigatorApiStreamingResponseError(
                    BROKEN_RESPONSE_STREAM_ERROR_MESSAGE
                )

    def stream_workflow_outputs(
        self, workflow: dict, verbose: bool = False
    ) -> Iterator[Message]:
        with requests.post(
            f"{self._api_endpoint}/v2/workflows/exec_streaming",
            json=workflow,
            headers=self._req_headers,
            stream=True,
        ) as response:
            _check_for_error_response(response)

            try:
                for output in response.iter_lines():
                    yield Message.from_dict(json.loads(output), raise_on_error=True)
            except ChunkedEncodingError:
                yield WorkflowInterruption(BROKEN_RESPONSE_STREAM_ERROR_MESSAGE)

    def submit_batch_workflow(
        self,
        workflow_config: dict,
        num_records: int,
        project_name: Optional[str] = None,
        workflow_id: Optional[str] = None,
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
                "workflow_id": workflow_id,
            },
            headers=self._req_headers,
        )
        _check_for_error_response(response)
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
            _check_for_error_response(response)

            try:
                yield response
            except ChunkedEncodingError:
                raise NavigatorApiStreamingResponseError(
                    BROKEN_RESPONSE_STREAM_ERROR_MESSAGE
                )

    def registry(self) -> list[dict]:
        response = requests.get(
            f"{self._api_endpoint}/v2/workflows/registry", headers=self._req_headers
        )
        _check_for_error_response(response)

        return response.json()["tasks"]


def _check_for_error_response(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except HTTPError:
        if 400 <= response.status_code < 500:
            raise NavigatorApiClientError(response.json())
        else:
            raise NavigatorApiServerError(response.json())


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
