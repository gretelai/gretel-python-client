import base64
import time
from functools import wraps
from typing import TYPE_CHECKING, Iterator, List, Optional

from gretel_client_v2.projects.common import ACTIVE_STATES, LogStatus, RestFields, Status

if TYPE_CHECKING:
    from gretel_client_v2.projects.models import Model
else:
    Model = None


class RecordHandlerError(Exception):
    ...


def _needs_remote_model(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if not hasattr(self, "_data"):
            raise RecordHandlerError("Does not have remote record handler details")
        return func(self, *args, **kwargs)

    return wrap


class RecordHandler:
    def __init__(self, model: Model, record_id: str = None):
        self.model = model
        self.project = model.project
        self._logs_iter_index = 0
        self._projects_api = self.model._projects_api
        self.record_id = record_id

    def create(
        self, params: Optional[dict], action: str, runner: str, data_source: Optional[str]
    ):
        handler = self.model._projects_api.create_record_handler(
            project_id=self.model.project.project_id,
            model_id=self.model.model_id,
            body={"params": params, "data_source": data_source},
            action=action,
            runner_mode="cloud" if runner == "cloud" else "manual",
        )
        self.action = action
        self._data = handler
        self.record_id = handler.get("data").get("handler").get("uid")
        return self._data

    @property
    def type(self) -> Optional[str]:
        if self.action == "generate":
            return "synthetics"
        if self.action == "transform":
            return "transforms"

    @property
    def worker_key(self) -> Optional[str]:
        if self._data:
            return self._data.get("worker_key")

    def _new_model_logs(self) -> List[dict]:
        if self.logs and len(self.logs) > self._logs_iter_index:
            next_logs = self.logs[self._logs_iter_index :]
            self._logs_iter_index += len(next_logs)
            return next_logs
        return []

    def _poll_record_handler(self):
        try:
            resp = self._projects_api.get_record_handler(
                project_id=self.project.name,
                model_id=self.model.model_id,
                logs="yes",
                record_handler_id=self.record_id,
            )
            self._data = resp.get("data")
        except Exception as ex:
            raise Exception(
                f"Cannot fetch model details for project {self.project.name} model {self.model.model_id}"
            ) from ex

    @property
    @_needs_remote_model
    def status(self) -> str:
        """Returns the status of the job"""
        return self._data.get("handler").get("status")

    @property
    @_needs_remote_model
    def logs(self):
        return self._data.get("logs")

    @property
    @_needs_remote_model
    def errors(self) -> str:
        return self._data.get("handler").get("error_msg")

    @property
    @_needs_remote_model
    def traceback(self) -> str:
        return base64.b64decode(self._data.get("handler").get("traceback")).decode(
            "utf-8"
        )

    def _check_predicate(self, start: float, wait: int = 0) -> bool:
        self._poll_record_handler()
        if self.status == "completed" or self.status == "error":
            return False
        if wait > 0 and time.time() - start > wait:
            return False
        return True

    def poll_logs_status(self, wait: int = 0) -> Iterator[LogStatus]:
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
                yield LogStatus(status=self.status, logs=logs, transitioned=transitioned)
            time.sleep(1)

        flushed_logs = self._new_model_logs()
        if len(flushed_logs) > 0 and current_status:
            yield LogStatus(status=current_status, logs=flushed_logs, transitioned=False)

        if self.status == "error":
            yield LogStatus(status=self.status, error=self.errors)
        else:
            yield LogStatus(status=self.status)

    def cancel(self):
        self._poll_record_handler()
        if self.status in ACTIVE_STATES:
            self._projects_api.update_model(
                project_id=self.project.project_id,
                model_id=self.model.model_id,
                body={RestFields.STATUS.value: Status.CANCELLED.value},
            )

    def delete(self):
        self._projects_api.delete_record_handler(
            project_id=self.project.project_id,
            model_id=self.model.model_id,
            record_handler_id=self.record_id,
        )
