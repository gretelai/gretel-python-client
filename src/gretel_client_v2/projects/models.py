"""
Classes and methods for working with Gretel Models
"""
import base64
import json
import time
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple, Union

import yaml
from smart_open import open

from gretel_client_v2.config import DEFAULT_RUNNER, RunnerMode
from gretel_client_v2.projects.helpers import Pathlike, validate_data_source
from gretel_client_v2.rest.api.projects_api import ProjectsApi

if TYPE_CHECKING:
    from gretel_client_v2.projects import Project
else:
    Project = None


BASE_BLUEPRINT_REPO = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel"

_ModelConfigPathT = Union[str, Path, dict]


class ModelConfigError(Exception):
    ...


class ModelError(Exception):
    ...


class ModelArtifactError(Exception):
    ...


MODEL_ARTIFACT_TYPES = ["model", "report", "report_json", "data_preview", "model_logs"]


def _resolve_config_short_path(config_path: str) -> dict:
    path = f"{BASE_BLUEPRINT_REPO}/{config_path}.yml"
    try:
        with open(path) as tpl:  # type:ignore
            return yaml.safe_load(tpl.read())
    except Exception as ex:
        raise ModelConfigError(
            f"Could not find or read the blueprint {config_path}"
        ) from ex


def _needs_remote_model(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if not hasattr(self, "_data"):
            raise ModelError("Does not have remote model details")
        return func(self, *args, **kwargs)

    return wrap


@dataclass
class Status:
    status: str
    transitioned: bool = False
    logs: List[dict] = field(default_factory=list)
    error: Optional[str] = None


def read_model_config(model_config: _ModelConfigPathT) -> dict:

    config = None
    if isinstance(model_config, dict):
        config = model_config

    # try and read model config from the local file system
    if not config:
        config_contents = None
        try:
            with open(model_config, "rb") as fc:  # type:ignore
                config_contents = fc.read()
        except FileNotFoundError:
            pass
        except Exception as ex:
            raise ModelConfigError(f"Could not read {model_config}") from ex

        # todo(dn): if there is a yaml or json parse error, we want to bubble
        # that error up to the user. first, we need to decide if the file is
        # supposed to be a yaml or json file.
        if config_contents:
            try:
                config = yaml.safe_load(config_contents)
            except Exception:
                pass

            try:
                config = json.loads(config_contents)
            except Exception:
                pass

    # try and read a model config from a blueprint short path
    if not config and isinstance(model_config, str):
        config = _resolve_config_short_path(model_config)

    if not config:
        raise ModelConfigError(f"Could not load model {model_config}")

    return config


class Model:
    """Represents a Gretel Model. This class can be used to train new
    models or run and lookup existing ones.
    """

    project: Project
    """The project associated with the model."""

    model_config: dict
    """Model config."""

    model_id: Optional[str] = None
    """Optional model id. If a model_id is specified, that model will be
    resolved from Gretel's API.
    """

    _projects_api: ProjectsApi
    """Project api rest bindings."""

    def __init__(
        self,
        project: Project,
        model_config: _ModelConfigPathT = None,
        model_id: str = None,
    ):
        self.project = project
        if model_config:
            self.model_config = read_model_config(model_config)
        self.model_id = model_id
        self._projects_api = project.projects_api
        if self.model_id:
            self._poll_model()
        self._logs_iter_index = 0

    def submit(
        self,
        runner_mode: RunnerMode = DEFAULT_RUNNER,
        dry_run: bool = False,
        upload_data_source: bool = False,
        _validate_data_source: bool = True
    ) -> dict:
        """Submit a model to be run.

        Args:
            runner_mode: Determines where to run the model. See ``RunnerMode``
                for a list of valid modes. Defaults to manual mode.
            dry_run: If set to True the model config will be sumbitted for
                validation, but won't be run.
            upload_data_source: If set to True the model's data source will
                be resolved and download to the host machine and then uploaded
                as a Gretel Cloud artifact.

        Raises:
            - ``ModelConfigError`` if the specified model config is invalid.
            - ``RuntimeError`` if the model is submitted more than once.
            - ``ApiException`` if there is a problem submitting the model to
                Gretel's api.
        """
        if not self.model_config:
            raise ModelConfigError("No model config exists to submit.")

        if self.model_id:
            raise RuntimeError("This model was already submitted.")

        if upload_data_source:
            self.upload_data_source(_validate=_validate_data_source)

        resp = self._projects_api.create_model(
            project_id=self.project.name,
            body=self.model_config,
            dry_run="yes" if dry_run else "no",
            runner_mode=runner_mode.value
            if runner_mode == RunnerMode.CLOUD
            else "manual",
        )

        self._data = resp.get("data").get("model")
        self._worker_key = resp.get("worker_key")
        self.model_id = self._data.get("uid")

        return self._data

    def get_artifacts(self) -> Iterator[Tuple[str, str]]:
        """List artifact links for all known artifact types."""
        for artifact in MODEL_ARTIFACT_TYPES:
            yield artifact, self.get_artifact_link(artifact)

    def get_artifact_link(self, artifact_type: str) -> str:
        """Retrieves a signed S3 link that will download the specified
        artifact type.

        Args:
            artifact_type: Artifact type to download
        """
        if artifact_type not in MODEL_ARTIFACT_TYPES:
            raise ModelArtifactError(
                f"{artifact_type} is invalid. Must be in {','.join(MODEL_ARTIFACT_TYPES)}"
            )
        art_resp = self._projects_api.get_model_artifact(
            project_id=self.project.name, model_id=self.model_id, type=artifact_type
        )
        return art_resp["data"]["url"]

    @property
    @_needs_remote_model
    def status(self) -> str:
        """Returns the status of the job"""
        return self._data.get("model").get("status")

    @property
    @_needs_remote_model
    def logs(self):
        return self._data.get("logs")

    @property
    @_needs_remote_model
    def errors(self) -> str:
        return self._data.get("model").get("error_msg")

    @property
    @_needs_remote_model
    def traceback(self) -> str:
        return base64.b64decode(self._data.get("model").get("traceback")).decode(
            "utf-8"
        )

    @property
    def model_type(self) -> str:
        try:
            return list(self.model_config["models"][0].keys())[0]
        except (IndexError, KeyError) as ex:
            raise ModelConfigError("Could not determine model type from config") from ex

    @property
    def external_data_source(self) -> bool:
        """Returns ``True`` if the data source is external to Gretel Cloud.
        If the data source is a Gretel Artifact, returns ``False``.
        """
        return not self.data_source.startswith("gretel_")

    @property
    def data_source(self) -> str:
        """Retrieves the configured data source from the model config"""
        try:
            return self.model_config["models"][0][self.model_type]["data_source"]
        except (IndexError, KeyError) as ex:
            raise ModelConfigError(
                "Could not get data source from model config"
            ) from ex

    @data_source.setter
    def data_source(self, data_source: str):
        self.model_config["models"][0][self.model_type]["data_source"] = data_source

    def delete(self) -> Optional[dict]:
        """Deletes the remote model."""
        if self.model_id:
            return self._projects_api.delete_model(
                project_id=self.project.name, model_id=self.model_id
            )

    def validate_data_source(self):
        """Test that the attached data source is a valid
        Csv or Json file. If the data source is a Gretel
        cloud artifact data validation will be skipped.

        Raises:
            - ``ModelArtifactError`` if the data source is not valid.
        """
        if not self.external_data_source:
            return
        try:
            validate_data_source(self.data_source)
        except Exception as ex:
            raise ModelArtifactError("Could not validate data source") from ex

    def upload_data_source(self, _validate: bool = True) -> str:
        """Resolves and uploads the data source specified in the
        model config.

        Returns:
            A Gretel artifact key
        """
        self.data_source = self.project.upload_artifact(self.data_source, _validate)
        return self.data_source

    def _poll_model(self):
        try:
            resp = self._projects_api.get_model(
                project_id=self.project.name, model_id=self.model_id, logs="yes"
            )
            self._data = resp.get("data")
        except Exception as ex:
            raise ModelError(
                f"Cannot fetch model details for project {self.project.name} model {self.model_id}"
            ) from ex

    def _new_model_logs(self) -> List[dict]:
        if self.logs and len(self.logs) > self._logs_iter_index:
            next_logs = self.logs[self._logs_iter_index :]
            self._logs_iter_index += len(next_logs)
            return next_logs
        return []

    def _check_predicate(self, start: float, wait: int = 0) -> bool:
        self._poll_model()
        if self.status == "completed" or self.status == "error":
            return False
        if wait > 0 and time.time() - start > wait:
            return False
        return True

    def poll_logs_status(self, wait: int = 0) -> Iterator[Status]:
        """Returns an iterator that can be used to tail the logs
        of a running Model

        Args:
            wait: The time in seconds to wait before closing the
                iterator. If wait is 0, the iterator will run until
                the model has reached a "completed"  or "error" state.
        """
        start = time.time()
        current_status = None
        while self._check_predicate(start, wait):
            logs = self._new_model_logs()
            if self.status != current_status or len(logs) > 0:
                transitioned = self.status != current_status
                current_status = self.status
                yield Status(status=self.status, logs=logs, transitioned=transitioned)
            time.sleep(1)

        flushed_logs = self._new_model_logs()
        if len(flushed_logs) > 0 and current_status:
            yield Status(status=current_status, logs=flushed_logs, transitioned=False)

        if self.status == "error":
            yield Status(status=self.status, error=self.errors)
        else:
            yield Status(status=self.status)

    def generate(self, input_data: Pathlike):
        if self.model_type != "synthetics":
            raise ModelError(f"Can generate records using model type: {self.model_type}")

    def transform(self, input_data: Pathlike):
        if self.model_type != "transform":
            raise ModelError(f"Can transforms records using model type: {self.model_type}")

    def classify(self, input_data: Pathlike):
        ...
