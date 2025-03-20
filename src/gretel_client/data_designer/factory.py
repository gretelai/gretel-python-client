from pathlib import Path

from gretel_client.data_designer import DataDesigner
from gretel_client.data_designer.types import DataColumnT, EvaluatorT, ModelSuite
from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.workflows.configs.tasks import ColumnConstraint
from gretel_client.workflows.configs.workflows import ModelConfig


class DataDesignerFactory:

    def __init__(
        self, gretel_resource_provider: GretelResourceProviderProtocol
    ) -> None:
        self._gretel_resource_provider = gretel_resource_provider

    def from_config(self, config: dict | str | Path) -> DataDesigner:
        data_designer_dataset = DataDesigner.from_config(
            self._gretel_resource_provider, config
        )
        return data_designer_dataset

    def new(
        self,
        *,
        model_suite: ModelSuite = ModelSuite.APACHE_2_0,
        model_configs: list[ModelConfig] | None = None,
        columns: dict[str, DataColumnT] | None = None,
        constraints: list[ColumnConstraint] | None = None,
        evaluators: dict[str, EvaluatorT] | None = None,
    ) -> DataDesigner:
        return DataDesigner(
            gretel_resource_provider=self._gretel_resource_provider,
            model_suite=model_suite,
            model_configs=model_configs,
            columns=columns,
            constraints=constraints,
            evaluators=evaluators,
        )
