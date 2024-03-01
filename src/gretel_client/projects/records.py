from __future__ import annotations

from typing import Any, List, Optional, Type, TYPE_CHECKING

from gretel_client.cli.utils.parser_utils import (
    DataSourceTypes,
    ref_data_factory,
    RefData,
    RefDataTypes,
)
from gretel_client.config import RunnerMode
from gretel_client.models.config import get_model_type_config
from gretel_client.projects.common import f, ModelRunArtifact
from gretel_client.projects.exceptions import (
    MaxConcurrentJobsException,
    RecordHandlerError,
    RecordHandlerNotFound,
)
from gretel_client.projects.jobs import GretelJobNotFound, Job, Status
from gretel_client.rest.exceptions import ApiException

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
        self._data_source = data_source
        self._params = params
        self._ref_data = ref_data
        super().__init__(model.project, JOB_TYPE, self.record_id)

    # kwargs are ignored; only present to support polymorphism in jobs.py
    def _submit(self, runner_mode: RunnerMode, **kwargs) -> RecordHandler:
        """Submits the record handler to be run."""
        body = {
            "params": self.params,
            "data_source": self.data_source,
            "ref_data": self.ref_data.ref_dict,
        }
        body = {key: value for key, value in body.items() if value is not None}
        provenance = self.project.session.context.job_provenance
        if len(provenance) > 0:
            body["provenance"] = provenance

        try:
            handler = self.model._projects_api.create_record_handler(
                project_id=self.model.project.project_guid,
                model_id=self.model.model_id,
                body=body,
                runner_mode=runner_mode.api_value,
            )
        except ApiException as ex:
            self._handle_submit_error(ex)

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
    def instance_type(self) -> str:
        """Return CPU or GPU based on the record handler's run requirements."""
        return get_model_type_config(self.model_type).run_instance_type

    @property
    def artifact_types(self) -> List[str]:
        """Returns a list of valid artifacts for the record handler."""
        return [a.value for a in ModelRunArtifact]

    @property
    def data_source(self) -> Optional[DataSourceTypes]:
        """Returns the data source with which the record handler was configured, if any.

        If the record handler has been submitted, returns the resolved artifact ID.
        Otherwise, returns the originally-supplied data_source argument."""
        data_source = self._data_source
        if self._data:
            data_source = self._get_config_field_from_data("data_source")
        return data_source

    @data_source.setter
    def data_source(self, data_source: DataSourceTypes) -> None:
        self._data_source = data_source

    @property
    def params(self) -> Optional[dict]:
        """Returns the params with which the record handler was configured, if any."""
        params = self._params
        if self._data:
            params = self._get_config_field_from_data("params")
        return params

    @params.setter
    def params(self, params: dict) -> None:
        self._params = params

    @property
    def ref_data(self) -> Optional[RefDataTypes]:
        """Returns the ref_data with which the record handler was configured, if any."""
        ref_data = self._ref_data
        if self._data:
            ref_data = self._get_config_field_from_data("ref_data")
        return ref_data_factory(ref_data)

    @ref_data.setter
    def ref_data(self, ref_data: dict) -> None:
        self._ref_data = ref_data

    def _get_config_field_from_data(self, field: str) -> Optional[Any]:
        return self._data.get(f.HANDLER, {}).get("config", {}).get(field)

    def _do_get_job_details(self, extra_expand: Optional[list[str]] = None):
        if not self.record_id:
            raise RecordHandlerError(
                "Record handler does not exist. Try calling create first."
            )
        expand = [f.LOGS]
        if extra_expand:
            expand.extend(extra_expand)
        return self._projects_api.get_record_handler(
            project_id=self.project.project_guid,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            expand=expand,
        )

    def _do_cancel_job(self):
        self._projects_api.update_record_handler(
            project_id=self.project.project_guid,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            body={f.STATUS: Status.CANCELLED.value},
        )

    def _do_get_artifact(self, artifact_key: str) -> str:
        return self.project.default_artifacts_handler.get_record_handler_artifact_link(
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
            artifact_type=artifact_key,
        )

    def delete(self):
        """Deletes the record handler."""
        self._projects_api.delete_record_handler(
            project_id=self.project.project_guid,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
        )
