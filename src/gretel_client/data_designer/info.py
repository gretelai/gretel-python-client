from functools import cached_property

from gretel_client.data_designer.exceptions import DataDesignerValidationError
from gretel_client.data_designer.types import ModelSuite
from gretel_client.data_designer.utils import get_sampler_params
from gretel_client.data_designer.viz_tools import (
    display_model_suite_info,
    display_sampler_table,
)
from gretel_client.workflows.configs.tasks import SamplerType
from gretel_client.workflows.manager import (
    LLMSuiteConfigWithGenerationParams,
    WorkflowManager,
)


class AIDDInfo:
    def __init__(self, model_suite: ModelSuite, workflow_manager: WorkflowManager):
        self._model_suite = model_suite
        self._workflow_manager = workflow_manager
        self._sampler_params = get_sampler_params()

    @cached_property
    def available_model_suites(self) -> dict[str, LLMSuiteConfigWithGenerationParams]:
        return {ms.suite_name: ms for ms in self._workflow_manager.get_model_suites()}

    @property
    def model_suite(self) -> None:
        self.display_model_suite(self._model_suite)

    @property
    def sampler_table(self) -> None:
        display_sampler_table(self._sampler_params)

    @property
    def sampler_types(self) -> list[str]:
        return [s.value for s in SamplerType]

    def display_model_suite(self, model_suite: ModelSuite | None = None) -> None:
        model_suite = model_suite or self._model_suite
        violations = self._workflow_manager.validate_model_suite(model_suite, [])
        if len(violations) > 0:
            raise DataDesignerValidationError(violations[0])
        display_model_suite_info(self.available_model_suites[model_suite])

    def display_sampler(self, sampler_type: SamplerType) -> None:
        title = f"{SamplerType(sampler_type).value.replace('_', ' ').title()} Sampler"
        display_sampler_table(
            {sampler_type: self._sampler_params[sampler_type]}, title=title
        )
