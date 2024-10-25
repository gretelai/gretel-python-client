from abc import ABC, abstractmethod
from typing import Optional

from pydantic import BaseModel

from gretel_client.navigator.client.interface import Client, ClientAdapter, TaskOutput
from gretel_client.navigator.client.utils import get_navigator_client


class Task(ABC):

    def __init__(
        self,
        config: BaseModel,
        workflow_label: Optional[str] = None,
        client: Optional[Client] = None,
    ):
        self.config = config
        self.workflow_label = workflow_label
        self._client = client or get_navigator_client()

    def _set_client(self, adapter: ClientAdapter):
        """Set client adapter for task execution.

        This is an internal method that is not useable by end users.
        """
        self._client = get_navigator_client(adapter)

    def _run(self, *inputs) -> TaskOutput:
        try:
            return self._client.run_task(
                name=self.name, config=self.config.model_dump(), inputs=list(inputs)
            )
        except Exception as e:
            print(e)

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run(self, *args, **kwargs) -> TaskOutput: ...
