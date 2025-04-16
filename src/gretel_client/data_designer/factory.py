from pathlib import Path

import gretel_client.data_designer.columns as columns
import gretel_client.data_designer.params as params

from gretel_client.data_designer import DataDesigner
from gretel_client.data_designer.types import ModelSuite
from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.workflows.configs.workflows import ModelConfig


class DataDesignerFactory:
    """Factory for creating DataDesigner instances.

    This class should always be used as an attribute of the Gretel object.

    Attributes:
        columns: Helper attribute for quick access to AIDD column objects.
        params: Helper attribute for quick access to AIDD parameter objects.

    Example::

        from gretel_client import Gretel

        gretel = Gretel(api_key="prompt")

        # Create a DataDesigner instance from scratch
        aidd = gretel.data_designer.new()

        # Create a DataDesigner instance from a configuration file
        aidd = gretel.data_designer.from_config("path/to/config.yaml")
    """

    columns = columns
    params = params

    def __init__(
        self, gretel_resource_provider: GretelResourceProviderProtocol
    ) -> None:
        self._gretel_resource_provider = gretel_resource_provider

    def from_config(self, config: dict | str | Path) -> DataDesigner:
        """Create a DataDesigner instance from a configuration.

        Args:
            config: Configuration as a dict or path to a YAML file.

        Returns:
            A DataDesigner instance configured with the given configuration.
        """
        data_designer_dataset = DataDesigner.from_config(
            self._gretel_resource_provider, config
        )
        return data_designer_dataset

    def new(
        self,
        *,
        model_suite: ModelSuite = ModelSuite.APACHE_2_0,
        model_configs: list[ModelConfig] | None = None,
    ) -> DataDesigner:
        """Create a new DataDesigner instance.

        Args:
            model_suite: Model suite for the synthetic data generation.
            model_configs: Configurations of the LLMs in the model suite.

        Returns:
            A new DataDesigner instance.
        """
        return DataDesigner(
            gretel_resource_provider=self._gretel_resource_provider,
            model_suite=model_suite,
            model_configs=model_configs,
        )
