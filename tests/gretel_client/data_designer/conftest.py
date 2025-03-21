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

person_samplers:
  some_dude:
    sex: Male
    locale: en_GB
  some_lady:
    sex: Female
    locale: fr_FR

evaluation_report:
  type: general

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
      type: llm-generated
      prompt: Write a description of python code in topic {topic} and domain {domain}
    - name: code
      type: llm-generated
      prompt: Write Python code that will be paired with the following prompt {text}
      model_alias: my_own
      data_config:
        type: code
        params:
          syntax: python
    - name: code_validation_result
      type: code-validation
      code_lang: python
      target_column: code
    - name: code_judge_result
      type: llm-judge
      prompt: You are an expert in Python programming and make appropriate judgement on the quality of the code.
      rubrics:
        - name: Pythonic
          description: Pythonic Code and Best Practices (Does the code follow Python conventions and best practices?)
          scoring:
            "4": The code exemplifies Pythonic principles, making excellent use of Python-specific constructs, standard library modules and programming idioms; follows all relevant PEPs.
            "3": The code closely follows Python conventions and adheres to many best practices; good use of Python-specific constructs, standard library modules and programming idioms.
            "2": The code generally follows Python conventions but has room for better alignment with Pythonic practices.
            "1": The code loosely follows Python conventions, with several deviations from best practices.
            "0": The code does not follow Python conventions or best practices, using non-Pythonic approaches.
        - name: Readability
          description: Readability and Maintainability (Is the Python code easy to understand and maintain?)
          scoring:
            "4": The code is excellently formatted, follows PEP 8 guidelines, is elegantly concise and clear, uses meaningful variable names, ensuring high readability and ease of maintenance; organizes complex logic well. Docstrings are given in a Google Docstring format.
            "3": The code is well-formatted in the sense of code-as-documentation, making it relatively easy to understand and maintain; uses descriptive names and organizes logic clearly.
            "2": The code is somewhat readable with basic formatting and some comments, but improvements are needed; needs better use of descriptive names and organization.
            "1": The code has minimal formatting, making it hard to understand; lacks meaningful names and organization.
            "0": The code is unreadable, with no attempt at formatting or description.

constraints:
    - target_column: age
      type: scalar_inequality
      params:
        operator: "<"
        rhs: 65
"""
