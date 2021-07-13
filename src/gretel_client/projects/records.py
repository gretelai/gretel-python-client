from typing import TYPE_CHECKING, Optional, List
from gretel_client.config import DEFAULT_RUNNER, RunnerMode

from gretel_client.projects.jobs import CPU, Status, Job
from gretel_client.projects.common import ModelRunArtifact, ModelType, YES, f

if TYPE_CHECKING:
    from gretel_client.projects.models import Model
else:
    Model = None


class RecordHandlerError(Exception):
    ...


JOB_TYPE = "handler"


class RecordHandler(Job):
    """Manages a model's record handler. After a model has been created
    and trained, a record handler may be used to \"run\" the model.

    Args:
        model: The model to generate a record handler for
        record_id: The id of an existing record handler.
    """
    def __init__(self, model: Model, record_id: str = None):
        self.model = model
        self.record_id = record_id
        super().__init__(model.project, JOB_TYPE, self.record_id)

    def submit(
        self,
        action: str,
        runner_mode: RunnerMode = DEFAULT_RUNNER,
        data_source: Optional[str] = None,
        params: Optional[dict] = None,
        upload_data_source: bool = False,
    ) -> dict:
        """Submits the record handler to be run."""
        if upload_data_source and data_source:
            data_source = self._upload_data_source(data_source)
        handler = self.model._projects_api.create_record_handler(
            project_id=self.model.project.project_id,
            model_id=self.model.model_id,
            body={"params": params, "data_source": data_source},
            action=action,
            runner_mode=RunnerMode.CLOUD.value
            if runner_mode == RunnerMode.CLOUD
            else RunnerMode.MANUAL.value,
        )
        self.action = action
        self._data: dict = handler[f.DATA]
        self.record_id = self._data[self.job_type][f.UID]
        self.worker_key = handler[f.WORKER_KEY]
        return self.print_obj

    def _upload_data_source(self, data_source: str) -> str:
        return self.project.upload_artifact(data_source)

    @property
    def container_image(self) -> str:
        return self._data.get(f.HANDLER).get(f.CONTAINER_IMAGE)

    @property
    def model_type(self) -> ModelType:
        """Returns the parent model type of the record handler."""
        return self.model.model_type

    @property
    def instance_type(self) -> str:
        """Return CPU or GPU based on the record handler's run requirements."""
        return CPU

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
            logs=YES,
            record_handler_id=self.record_id,
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
