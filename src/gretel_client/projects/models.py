"""
Classes and methods for working with Gretel Models
"""

from __future__ import annotations

import copy
import json

from pathlib import Path
from typing import Iterator, List, Optional, Type, TYPE_CHECKING, Union

import requests.exceptions
import yaml

from smart_open import open

from gretel_client.cli.utils.parser_utils import (
    DataSourceTypes,
    ref_data_factory,
    RefData,
    RefDataTypes,
)
from gretel_client.config import get_logger, RunnerMode
from gretel_client.dataframe import _DataFrameT
from gretel_client.models.config import get_model_type_config
from gretel_client.projects.common import f, ModelArtifact, NO, YES
from gretel_client.projects.exceptions import (
    GretelJobNotFound,
    MaxConcurrentJobsException,
    ModelConfigError,
    ModelNotFoundError,
)
from gretel_client.projects.jobs import Job, Status
from gretel_client.projects.records import RecordHandler
from gretel_client.rest.exceptions import ApiException

if TYPE_CHECKING:
    from gretel_client.projects import Project
else:
    Project = None


BASE_BLUEPRINT_REPO = "https://raw.githubusercontent.com/gretelai/gretel-blueprints/main/config_templates/gretel"

_ModelConfigPathT = Union[str, Path, dict]


def _maybe_warn_deprecation(config: str) -> None:
    if not config.startswith("# deprecated"):
        return

    # A generic message in the event parsing the message in the config fails
    warn_msg = "This config will be deprecated soon. Please see the config itself for alternative options."
    try:
        warn_msg = config.split("\n")[0].split(":")[-1].strip()
    except Exception:
        pass

    get_logger(__name__).warning(warn_msg)


def _resolve_config_short_path(
    config_path: str, *, base_url: str = BASE_BLUEPRINT_REPO
) -> dict:
    path = f"{base_url}/{config_path}.yml"
    try:
        with open(path) as tpl:  # type:ignore
            tpl_str = tpl.read()
            _maybe_warn_deprecation(tpl_str)
            return yaml.safe_load(tpl_str)
    except requests.exceptions.HTTPError as ex:
        raise ModelConfigError(
            f"Could not find or read the blueprint {config_path}"
        ) from ex


def read_model_config(
    model_config: _ModelConfigPathT, *, base_url: str = BASE_BLUEPRINT_REPO
) -> dict:
    """
    Load a Gretel configuration into a dictionary.

    Args:
        model_config: This argument may be a string to a file on disk or a Gretel configuration template
            string such as "synthetics/default". First, this function will treat string input as a
            location on disk and attempt to read the file and parse it as YAML or JSON. If this is
            successful, a dict of the config is returned. If the provided `model_config` str is not
            a file on disk, the function will attempt to resolve the config as a shortcut-path
            from URL provided by `base_url.`

        base_url: A base HTTP URL that should be use to construct a fully qualified path
            to a configuration template. This URL will be used to resolve a config shortcut string
            to the fully qualified URL.
    """

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
        try:
            config = _resolve_config_short_path(model_config, base_url=base_url)
        except ModelConfigError:
            pass

    if not config:
        raise ModelConfigError(f"Could not find model config '{model_config}'")

    return config


JOB_TYPE = "model"


class Model(Job):
    """Represents a Gretel Model. This class can be used to train new
    models or run and lookup existing ones.
    """

    model_id: Optional[str]
    project: Project
    model_config: _ModelConfigPathT
    _local_model_config_path: Optional[Path]
    _not_found_error: Type[GretelJobNotFound] = ModelNotFoundError

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
    ) -> Model:
        """Submits a model to be run.

        Args:
            runner_mode: Determines where to run the model. See ``RunnerMode``
                for a list of valid modes. Local mode is not explicitly supported.
            dry_run: If set to True the model config will be submitted for
                validation, but won't be run.

        Raises:
            - ``ModelConfigError`` if the specified model config is invalid.
            - ``RuntimeError`` if the model is submitted more than once.
            - ``ApiException`` if there is a problem submitting the model to
                Gretel's api. If the problem was due specifically to reaching the.
                limit on concurrent jobs, raises ``MaxConcurrentJobsException`` subclass
        """
        if not self._local_model_config:
            raise ModelConfigError("No model config exists to submit.")

        if self.model_id:
            raise RuntimeError("This model was already submitted.")

        body = self._local_model_config
        provenance = self.project.session.context.job_provenance
        if len(provenance) > 0:
            body = {
                "config": body,
                "provenance": provenance,
            }

        try:
            resp = self._projects_api.create_model(
                project_id=self.project.project_guid,
                body=body,
                dry_run=YES if dry_run else NO,
                runner_mode=runner_mode.api_value,
            )
        except ApiException as ex:
            self._handle_submit_error(ex)

        self._data: dict = resp[f.DATA]
        self.worker_key = resp[f.WORKER_KEY]
        self.model_id = self._data[f.MODEL][f.UID]
        return self

    def _do_get_artifact(self, artifact_type: str) -> str:
        return self.project.default_artifacts_handler.get_model_artifact_link(
            model_id=self.model_id,
            artifact_type=artifact_type,
        )

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
        return get_model_type_config(self.model_type).train_instance_type

    @property
    def model_config(self) -> dict:
        """Returns the model config used to create the model."""
        return self._data[f.MODEL]["config"] if self._data else self._local_model_config

    @property
    def model_type(self) -> str:
        """Returns the type of model. Eg synthetics, transforms or classify."""
        try:
            return list(self.model_config["models"][0].keys())[0]
        except (IndexError, KeyError) as ex:
            raise ModelConfigError(
                "Could not determine model type from config."
            ) from ex

    @property
    def data_source(self) -> Optional[DataSourceTypes]:
        """Retrieves the configured data source from the model config.

        If the model config has a local data_source we'll try and resolve
        that path relative to the location of the model config.
        """
        try:
            model_config = self.model_config["models"][0][self.model_type]

            data_source = model_config.get("data_source")
            if data_source is None:
                return None

            if isinstance(data_source, list):
                data_source = data_source[0]
            if isinstance(data_source, _DataFrameT):
                return data_source
            if self._local_model_config_path and not data_source.startswith("gretel_"):
                data_source_path = self._local_model_config_path.parent / data_source
                if data_source_path.is_file():
                    return str(data_source_path)
            return data_source
        except (IndexError, KeyError) as ex:
            raise ModelConfigError(
                "Could not get data source from model config."
            ) from ex

    @data_source.setter
    def data_source(self, data_source: DataSourceTypes):
        """Configures a new data source for the model."""
        if self.model_id:
            raise RuntimeError(
                "Cannot update data source after the model has been submitted."
            )
        self.model_config["models"][0][self.model_type]["data_source"] = data_source

    @property
    def ref_data(self) -> RefData:
        """
        Retrieves configured ref data from the model config. If there are local ref data
        sources we will try and resolve that path relative to the location of the model config.
        """
        try:
            ref_data_dict = self.model_config["models"][0][self.model_type].get(
                "ref_data"
            )
            ref_data = ref_data_factory(ref_data_dict)
            if ref_data.is_cloud_data or (
                ref_data.ref_dict
                and isinstance(list(ref_data.ref_dict.values())[0], _DataFrameT)
            ):
                return ref_data

            # Below here, we will mutate `ref_data_dict` and re-create our instance

            if self._local_model_config_path:
                for key, data_source in ref_data.ref_dict.items():
                    data_source_path = (
                        self._local_model_config_path.parent / data_source
                    )  # type: Path
                    # If the path is a file, overwrite the original data source location
                    if data_source_path.is_file():
                        ref_data_dict[key] = str(data_source_path)

            return ref_data_factory(ref_data_dict)
        except (IndexError, KeyError):
            return ref_data_factory()

    @ref_data.setter
    def ref_data(self, ref_data: RefData):
        if self.model_id:
            raise RuntimeError(
                "Cannot update ref data after the model has been submitted."
            )
        self.model_config["models"][0][self.model_type]["ref_data"] = copy.deepcopy(
            ref_data.ref_dict
        )

    @property
    def name(self) -> Optional[str]:
        """Gets the name of the model. If no name is specified, a
        random name will be selected when the model is submitted
        to the backend.

        :getter: Returns the model name.
        :setter: Sets the model name.
        """
        return self.model_config.get("name")

    @name.setter
    def name(self, new_name: str):
        """Updates the name of the model.

        Args:
            new_name: The new name of the model.
        """
        self.model_config["name"] = new_name

    def delete(self) -> Optional[dict]:
        """Deletes the remote model."""
        if self.model_id:
            return self._projects_api.delete_model(
                project_id=self.project.project_guid, model_id=self.model_id
            )

    def validate_data_source(self):
        """Tests that the attached data source is a valid
        CSV or JSON file. If the data source is a Gretel
        cloud artifact OR a hybrid artifact and the runner mode is hybrid,
        data validation will be skipped.

        Raises:
            :class:`~gretel_client.projects.exceptions.DataSourceError` if the
                file can't be opened.
            :class:`~gretel_client.projects.exceptions.DataValidationError` if
                the data isn't valid CSV or JSON.
        """
        if self.data_source is None:
            return

        if not self.external_data_source:
            return

        self.project.default_artifacts_handler.validate_data_source(self.data_source)

    def validate_ref_data(self):
        if not self.external_ref_data:
            return
        ref_data = self.ref_data
        for data_source in ref_data.values:
            self.project.default_artifacts_handler.validate_data_source(data_source)

    def __repr__(self) -> str:
        return f"Model(id={self.model_id}, project={self.project.project_guid})"

    def _do_get_job_details(self, extra_expand: Optional[list[str]] = None):
        expand = [f.LOGS]
        if extra_expand:
            expand.extend(extra_expand)
        return self._projects_api.get_model(
            project_id=self.project.project_guid, model_id=self.model_id, expand=expand
        )

    def create_record_handler_obj(
        self,
        data_source: Optional[DataSourceTypes] = None,
        params: Optional[dict] = None,
        ref_data: Optional[RefDataTypes] = None,
    ) -> RecordHandler:
        """Creates a new record handler for the model.

        Args:
            data_source: A data source to upload to the record handler.
            params: Any custom params for the record handler. These params
                are specific to the upstream model.
        """
        return RecordHandler(
            self, data_source=data_source, params=params, ref_data=ref_data
        )

    def get_record_handler(self, record_id: str) -> RecordHandler:
        return RecordHandler(model=self, record_id=record_id)

    def _do_cancel_job(self):
        return self._projects_api.update_model(
            project_id=self.project.project_id,
            model_id=self.model_id,
            body={f.STATUS: Status.CANCELLED.value},
        )

    def get_record_handlers(self) -> Iterator[RecordHandler]:
        """Returns a list of record handlers associated with the model."""
        for status in Status:
            # Setting the start value for offset in each query, limiting each result set to 10 records
            offset = 0
            limit = 10
            while True:
                handlers = (
                    self._projects_api.query_record_handlers(
                        project_id=self.project.project_id,
                        model_id=self.model_id,
                        status=status.value,
                        skip=offset,
                        limit=limit,
                    )
                    .get("data")
                    .get("handlers")
                )
                # will break once no more handlers remain in the result set in the last pagination
                if len(handlers) > 0:
                    for handler in handlers:
                        yield RecordHandler(self, record_id=handler["uid"])
                    offset += limit
                else:
                    break
