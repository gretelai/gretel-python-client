from __future__ import annotations

from typing import List, Optional, Type, TYPE_CHECKING

from gretel_client.cli.utils.parser_utils import (
    DataSourceTypes,
    ref_data_factory,
    RefData,
    RefDataTypes,
)
from gretel_client.config import DEFAULT_RUNNER, RunnerMode
from gretel_client.models.config import get_model_type_config
from gretel_client.projects.common import f, ModelRunArtifact
from gretel_client.projects.exceptions import RecordHandlerError, RecordHandlerNotFound
from gretel_client.projects.jobs import GretelJobNotFound, Job, Status

if TYPE_CHECKING:
    from gretel_client.projects.models import Model


JOB_TYPE = "handler"


class RecordHandler(Job):
    """Manages a model's record handler. After a model has been created
    and trained, a record handler may be used to \"run\" the model.

    Args:
        model: The model to generate a record handler for
        record_id: The id of an existing record handler.
    """

    _not_found_error: Type[GretelJobNotFound] = RecordHandlerNotFound

    def __init__(
        self,
        model: Model,
        *,
        record_id: str = None,
        data_source: Optional[DataSourceTypes] = None,
        params: Optional[dict] = None,
        ref_data: Optional[RefDataTypes] = None,
    ):
        self.model = model
        self.record_id = record_id
        self.data_source = data_source
        self.params = params
        self.ref_data = ref_data_factory(ref_data)
        super().__init__(model.project, JOB_TYPE, self.record_id)

    def _submit(
        self, runner_mode: RunnerMode = DEFAULT_RUNNER, **kwargs
    ) -> RecordHandler:
        """Submits the record handler to be run."""

        # todo: we can drop the kwarg accessors after the Job.submit
        # method is deprecated.
        action = kwargs.get("action", self.action)
        data_source = kwargs.get("data_source", self.data_source)
        ref_data = kwargs.get("ref_data", self.ref_data)
        if not isinstance(ref_data, RefData):
            ref_data = ref_data_factory(ref_data)
        params = kwargs.get("params", self.params)
        upload_data_source = kwargs.get("upload_data_source", False)
        _default_manual = kwargs.get("_default_manual", False)

        if runner_mode == RunnerMode.LOCAL and not _default_manual:
            raise ValueError("Cannot use local mode")

        if upload_data_source and data_source:
            data_source = self._upload_data_source(data_source)

        if upload_data_source and not ref_data.is_empty:
            ref_data = self._upload_ref_data(ref_data)

        # If the runner mode is NOT set to cloud mode, check if we should
        # fall back to manual mode, this is useful for when running local
        # mode from the CLI.
        if runner_mode != RunnerMode.CLOUD and _default_manual:
            runner_mode = RunnerMode.MANUAL

        optional_kwargs = {}
        if action:
            optional_kwargs["action"] = action

        body = {
            "params": params,
            "data_source": data_source,
            "ref_data": ref_data.ref_dict,
        }
        body = {key: value for key, value in body.items() if value is not None}

        handler = self.model._projects_api.create_record_handler(
            project_id=self.model.project.project_id,
            model_id=self.model.model_id,
            body=body,
            runner_mode=runner_mode.value,
            **optional_kwargs,
        )

        self._data: dict = handler[f.DATA]
        self.record_id = self._data[self.job_type][f.UID]
        self.worker_key = handler[f.WORKER_KEY]
        return self

    def _upload_data_source(self, data_source: DataSourceTypes) -> str:
        return self.project.upload_artifact(data_source)

    def _upload_ref_data(self, ref_data: RefData) -> RefData:
        uploaded_refs = {}
        for key, data_source in ref_data.ref_dict.items():
            artifact_key = self.project.upload_artifact(data_source)
            uploaded_refs[key] = artifact_key
        return RefData(uploaded_refs)

    @property
    def container_image(self) -> str:
        return self._data.get(f.HANDLER).get(f.CONTAINER_IMAGE)

    @property
    def model_type(self) -> str:
        """Returns the parent model type of the record handler."""
        return self.model.model_type

    @property
    def action(self) -> Optional[str]:
        return get_model_type_config(self.model_type).action_name

    @property
    def instance_type(self) -> str:
        """Return CPU or GPU based on the record handler's run requirements."""
        return get_model_type_config(self.model_type).run_instance_type

    @property
    def artifact_types(self) -> List[str]:
        """Returns a list of valid artifacts for the record handler."""
        return [a.value for a in ModelRunArtifact]

    def _do_get_job_details(self):
        if not self.record_id:
            raise RecordHandlerError(
                "Record handler does not exist. Try calling create first."
            )
        return self._projects_api.get_record_handler(
            project_id=self.project.name,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            expand=[f.LOGS],
        )

    def _do_cancel_job(self):
        self._projects_api.update_record_handler(
            project_id=self.project.project_id,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            body={f.STATUS: Status.CANCELLED.value},
        )

    def _do_get_artifact(self, artifact_key: str) -> str:
        resp = self._projects_api.get_record_handler_artifact(
            project_id=self.project.name,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            type=artifact_key,
        )
        return resp[f.DATA][f.URL]

    def delete(self):
        """Deletes the record handler."""
        self._projects_api.delete_record_handler(
            project_id=self.project.project_id,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
        )
