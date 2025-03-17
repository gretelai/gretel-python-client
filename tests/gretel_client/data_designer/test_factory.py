from unittest.mock import MagicMock

from gretel_client.data_designer import DataDesigner, DataDesignerFactory
from gretel_client.data_designer.types import ModelSuite


def test_data_designer_factory_from_config(stub_aidd_config_str):
    factory = DataDesignerFactory(gretel_resource_provider=MagicMock())
    dd = factory.from_config(stub_aidd_config_str)
    assert isinstance(dd, DataDesigner)
    assert dd.model_suite == ModelSuite.APACHE_2_0


def test_data_designer_factory_new():
    factory = DataDesignerFactory(gretel_resource_provider=MagicMock())
    dd = factory.new(model_suite=ModelSuite.LLAMA_3_x)
    assert isinstance(dd, DataDesigner)
    assert dd.model_suite == ModelSuite.LLAMA_3_x
