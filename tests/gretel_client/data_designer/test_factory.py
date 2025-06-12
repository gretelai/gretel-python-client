from unittest.mock import MagicMock, patch

from gretel_client.data_designer import DataDesigner, DataDesignerFactory
from gretel_client.data_designer.types import ModelSuite


def test_data_designer_factory_from_config(stub_aidd_config_str):
    factory = DataDesignerFactory(
        api_factory=MagicMock(), gretel_resource_provider=MagicMock()
    )
    dd = factory.from_config(stub_aidd_config_str)
    assert isinstance(dd, DataDesigner)
    assert dd.model_suite == ModelSuite.APACHE_2_0


def test_data_designer_factory_new():
    factory = DataDesignerFactory(
        api_factory=MagicMock(), gretel_resource_provider=MagicMock()
    )
    dd = factory.new(model_suite=ModelSuite.LLAMA_3_x)
    assert isinstance(dd, DataDesigner)
    assert dd.model_suite == ModelSuite.LLAMA_3_x


def test_data_designer_factory_create_new_connection():
    mock_connections_api = MagicMock()
    mock_connections_api.create_connection.return_value = MagicMock(
        id="c_mock-connection-id"
    )
    mock_api_factory = MagicMock()
    mock_api_factory.get_api.return_value = mock_connections_api
    factory = DataDesignerFactory(
        api_factory=mock_api_factory,
        gretel_resource_provider=MagicMock(project_id="proj_mock-project-id"),
    )
    assert (
        factory.create_api_key_connection(
            name="test", api_base="https://foo.com/v1/", api_key="bar"
        )
        == "c_mock-connection-id"
    )
