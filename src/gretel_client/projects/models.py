"""
Classes and methods for working with Gretel Models
"""
from __future__ import annotations

import json
import logging

from pathlib import Path
from typing import Iterator, List, Optional, TYPE_CHECKING, Union

import yaml

from smart_open import open

from gretel_client.config import RunnerMode
from gretel_client.projects.common import (
    f,
    ModelArtifact,
    ModelType,
    NO,
    validate_data_source,
    YES,
)
from gretel_client.projects.jobs import CPU, GPU, Job, Status
from gretel_client.projects.records import RecordHandler

if TYPE_CHECKING:
    from gretel_client.projects import Project
else:
    Project = None


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BASE_BLUEPRINT_REPO = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel"

_ModelConfigPathT = Union[str, Path, dict]


class ModelConfigError(Exception):
    ...


class ModelError(Exception):
    ...


class ModelArtifactError(Exception):
    ...


def _resolve_config_short_path(config_path: str) -> dict:
    path = f"{BASE_BLUEPRINT_REPO}/{config_path}.yml"
    try:
        with open(path) as tpl:  # type:ignore
            return yaml.safe_load(tpl.read())
    except Exception as ex:
        raise ModelConfigError(
            f"Could not find or read the blueprint {config_path}"
        ) from ex


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

        if config_contents:
            valid_config_contents = False
            try:
                config = yaml.safe_load(config_contents)
                valid_config_contents = True
            except Exception:
                pass

            try:
                config = json.loads(config_contents)
                valid_config_contents = True
            except Exception:
                pass

            if not valid_config_contents:
                # The provided config was a valid path on the local
                # fs, but we could not load the config as YAML or JSON
                raise ModelConfigError(
                    "The provided config file is not valid YAML or JSON!"
                )

    # try and read a model config from a blueprint short path
    if not config and isinstance(model_config, str):
        config = _resolve_config_short_path(model_config)

    if not config:
        raise ModelConfigError(f"Could not load model {model_config}")

    return config


JOB_TYPE = "model"


class Model(Job):
    """Represents a Gretel Model. This class can be used to train new
    models or run and lookup existing ones.
    """

    def __init__(
        self,
        project: Project,
        model_config: _ModelConfigPathT = None,
        model_id: str = None,
    ):
        self._local_model_config_path = None
        if model_config:
            self._local_model_config = read_model_config(model_config)
            if isinstance(model_config, (str, Path)):
                self._local_model_config_path = Path(model_config)
        self.model_id = model_id
        super().__init__(project, JOB_TYPE, model_id)

    def _submit(
        self,
        runner_mode: RunnerMode,
        dry_run: bool = False,
        upload_data_source: bool = False,
        _validate_data_source: bool = True,
        _default_manual: bool = False,
    ) -> Model:
        """Submit a model to be run.

        Args:
            runner_mode: Determines where to run the model. See ``RunnerMode``
                for a list of valid modes. Local mode is not explicitly supported.
            dry_run: If set to True the model config will be sumbitted for
                validation, but won't be run.
            upload_data_source: If set to True the model's data source will
                be resolved and download to the host machine and then uploaded
                as a Gretel Cloud artifact. This is enabled by default to
                ease UX for SDK users. This flag will be ignored if the
                runner mode is not "cloud."

        Raises:
            - ``ModelConfigError`` if the specified model config is invalid.
            - ``RuntimeError`` if the model is submitted more than once.
            - ``ApiException`` if there is a problem submitting the model to
                Gretel's api.
        """
        if not self._local_model_config:
            raise ModelConfigError("No model config exists to submit.")

        if self.model_id:
            raise RuntimeError("This model was already submitted.")

        if not isinstance(runner_mode, RunnerMode):
            raise ValueError("Invalid runner_mode type, must be str or RunnerMode enum")

        if runner_mode == RunnerMode.LOCAL and not _default_manual:
            raise ValueError("Cannot use local mode")

        if upload_data_source and runner_mode == RunnerMode.CLOUD:
            self.upload_data_source(_validate=_validate_data_source)

        # If the runner mode is NOT set to cloud mode, check if we should
        # fall back to manual mode, this is useful for when running local
        # mode from the CLI.
        if runner_mode != RunnerMode.CLOUD and _default_manual:
            runner_mode = RunnerMode.MANUAL

        resp = self._projects_api.create_model(
            project_id=self.project.name,
            body=self._local_model_config,
            dry_run=YES if dry_run else NO,
            runner_mode=runner_mode.value,
        )

        self._data: dict = resp[f.DATA]
        self.worker_key = resp[f.WORKER_KEY]
        self.model_id = self._data[f.MODEL][f.UID]
        return self

    def _do_get_artifact(self, artifact_type: str) -> str:
        art_resp = self._projects_api.get_model_artifact(
            project_id=self.project.name, model_id=self.model_id, type=artifact_type
        )
        return art_resp["data"]["url"]

    @property
    def container_image(self) -> str:
        return self._data.get(f.MODEL).get(f.CONTAINER_IMAGE)

    @property
    def artifact_types(self) -> List[str]:
        """Returns a list of artifact types associated with the model."""
        return [a.value for a in ModelArtifact]

    @property
    def is_cloud_model(self):
        """Returns ``True`` if the model was created to run in Gretel's
        Cloud. ``False`` otherwise."""
        return self._data["model"]["runner_mode"] == "cloud"

    @property
    def instance_type(self) -> str:
        """Returns CPU or GPU based on the model being trained."""
        return GPU if self.model_type == ModelType.SYNTHETICS else CPU

    @property
    def model_config(self) -> dict:
        """Returns the model config used to create the model."""
        return self._data[f.MODEL]["config"] if self._data else self._local_model_config

    @property
    def model_type(self) -> ModelType:
        """Returns the type of model. Eg synthetics, transforms or classify."""
        try:
            return ModelType(list(self.model_config["models"][0].keys())[0])
        except (IndexError, KeyError) as ex:
            raise ModelConfigError("Could not determine model type from config") from ex

    @property
    def data_source(self) -> str:
        """Retrieves the configured data source from the model config.

        If the model config has a local data_source we'll try and resolve
        that path relative to the location of the model config.
        """
        try:
            data_source = self.model_config["models"][0][self.model_type]["data_source"]
            if isinstance(data_source, list):
                data_source = data_source[0]
            if self._local_model_config_path and not data_source.startswith("gretel_"):
                data_source_path = self._local_model_config_path.parent / data_source
                if data_source_path.is_file():
                    return str(data_source_path)
            return data_source
        except (IndexError, KeyError) as ex:
            raise ModelConfigError(
                "Could not get data source from model config"
            ) from ex

    @data_source.setter
    def data_source(self, data_source: str):
        """Configure a new data source for the model."""
        if self.model_id:
            raise RuntimeError(
                "Cannot update data source after the model has been submitted"
            )
        self.model_config["models"][0][self.model_type]["data_source"] = data_source

    def delete(self) -> Optional[dict]:
        """Deletes the remote model."""
        if self.model_id:
            for handler in self.get_record_handlers():
                handler.delete()
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

    def __repr__(self) -> str:
        return f"Model(id={self.model_id}, project={self.project.name})"

    def _do_get_job_details(self):
        return self._projects_api.get_model(
            project_id=self.project.name, model_id=self.model_id, logs="yes"
        )

    def create_record_handler_obj(
        self, data_source: Optional[str] = None, params: Optional[dict] = None
    ) -> RecordHandler:
        """Creates a new record handler for the model.

        Args:
            data_source: A data source to upload to the record handler.
             params: Any custom params for the record handler. These params
                are specific to the upstream model.
        """
        return RecordHandler(self, data_source=data_source, params=params)

    def _do_cancel_job(self):
        return self._projects_api.update_model(
            project_id=self.project.project_id,
            model_id=self.model_id,
            body={f.STATUS: Status.CANCELLED.value},
        )

    def get_record_handlers(self) -> Iterator[RecordHandler]:
        """Returns a list of record handlers associated with the model."""
        for status in Status:
            for handler in (
                self._projects_api.query_record_handlers(
                    project_id=self.project.project_id,
                    model_id=self.model_id,
                    status=status.value,
                )
                .get("data")
                .get("handlers")
            ):
                yield RecordHandler(self, record_id=handler["uid"])
