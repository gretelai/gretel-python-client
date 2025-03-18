from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from gretel_client.navigator_client_protocols import GretelResourceProviderProtocol
from gretel_client.workflows.manager import WorkflowBuilder, WorkflowManager


@dataclass
class MockLowLevelSdkResources:
    mock_workflow_builder: WorkflowBuilder
    mock_workflow_manager: WorkflowManager
    mock_resource_provider: GretelResourceProviderProtocol


@pytest.fixture()
def mock_low_level_sdk_resources() -> MockLowLevelSdkResources:
    mock_workflow_builder = MagicMock(spec=WorkflowBuilder)
    mock_workflow_manager = MagicMock(spec=WorkflowManager)
    mock_workflow_manager.builder.return_value = mock_workflow_builder
    mock_resource_provider = MagicMock(spec=GretelResourceProviderProtocol)
    mock_resource_provider.workflows = mock_workflow_manager
    return MockLowLevelSdkResources(
        mock_workflow_builder=mock_workflow_builder,
        mock_workflow_manager=mock_workflow_manager,
        mock_resource_provider=mock_resource_provider,
    )


@pytest.fixture
def stub_aidd_config_str() -> str:
    return """
model_suite: apache-2.0

model_configs:
  - alias: my_own_code_model
    model_name: gretel-qwen25-coder-7b-instruct
    generation_parameters:
      temperature:
        distribution_type: uniform
        params:
            low: 0.5
            high: 0.9
      top_p:
        distribution_type: manual
        params:
            values: [0.1, 0.2, 0.33]
            weights: [0.3, 0.2, 0.50]

seed_dataset:
  file_id: file_123
  sampling_strategy: shuffle
  with_replacement: true

columns:
    - name: code_id
      type: uuid
      params:
        prefix: code_
        short_form: true
        uppercase: true
    - name: age
      type: uniform
      params:
        low: 35
        high: 88
    - name: domain
      type: category
      params:
        values: [Healthcare, Finance, Education, Government]
    - name: topic
      type: category
      params:
        values: [Web Development, Data Science, Machine Learning, Cloud Computing]
    - name: text
      prompt: Write a description of python code in topic {topic} and domain {domain}
    - name: code
      prompt: Write Python code that will be paired with the following prompt {text}
      model_alias: my_own
      data_config:
        type: code
        params:
          syntax: python
constraints:
    - target_column: age
      type: scalar_inequality
      params:
        operator: "<"
        rhs: 65
validators:
    - type: code
      settings:
        code_lang: python
        target_columns: [code]
        result_columns: [code_is_valid]
evaluators:
    - type: judge_with_llm
      settings:
        judge_template_type: text_to_python
        text_column: text
        code_column: code
    - type: general
      settings:
        llm_judge_column: text_to_python
"""
