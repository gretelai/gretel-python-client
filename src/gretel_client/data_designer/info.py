from functools import cached_property

from gretel_client.data_designer.exceptions import DataDesignerValidationError
from gretel_client.data_designer.types import ModelSuite
from gretel_client.data_designer.viz_tools import display_model_suite_info
from gretel_client.workflows.manager import (
    LLMSuiteConfigWithGenerationParams,
    WorkflowManager,
)


class AIDDInfo:

    def __init__(self, model_suite: ModelSuite, workflow_manager: WorkflowManager):
        self._model_suite = model_suite
        self._workflow_manager = workflow_manager

    @cached_property
    def available_model_suites(self) -> dict[str, LLMSuiteConfigWithGenerationParams]:
        return {ms.suite_name: ms for ms in self._workflow_manager.get_model_suites()}

    @property
    def model_suite(self) -> None:
        display_model_suite_info(self.available_model_suites[self._model_suite])

    def display_model_suite(self, model_suite: ModelSuite | None = None) -> None:
        model_suite = model_suite or self._model_suite
        violations = self._workflow_manager.validate_model_suite(model_suite, [])
        if len(violations) > 0:
            raise DataDesignerValidationError(violations[0])
        display_model_suite_info(self.available_model_suites[model_suite])
