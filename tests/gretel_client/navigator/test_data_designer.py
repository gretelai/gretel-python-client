from unittest.mock import MagicMock, patch

import pytest

from gretel_client.config import get_session_config
from gretel_client.navigator import DataDesigner, DataDesignerFromSampleRecords


@pytest.fixture()
def fixture_config() -> str:
    return """\
model_suite: apache-2.0

special_system_instructions: >-
  You are an expert at writing, analyzing and editing SQL queries. You know what
  a high-quality, clean, efficient, and maintainable SQL code looks like. You
  excel at transforming natural language into SQL, as well as SQL back into
  natural language. Your job is to assist the user with their SQL-related tasks.

categorical_seed_columns:
  - name: domain
    description: Major industry domain or sector that relies on robust data solutions
    values: [Healthcare, Finance, Education, Science and Technology, Environmental Science, Government]
    subcategories:
      - name: domain_description
        description: High-level description of the domain, highlighting various types of data relevant to writing SQL
        num_new_values_to_generate: 1
      - name: topic
        description: Key topics that professional SQL developers care about in the given domain
        num_new_values_to_generate: 5

generated_data_columns:
  - name: sql_prompt
    generation_prompt: >-
        Create a natural language prompt to generate SQL in the field of {domain},
        specifically about the topic of {topic}. Feel free to ask for data that
        focus on a smaller subject within the scope of {domain_description}.
    columns_to_list_in_prompt: all_categorical_seed_columns

  - name: sql_context
    generation_prompt: >-
        You are a data and SQL expert in the {domain} domain. \n

        Generate SQL statements that create tables and views that already exist in
        a database that aligns with {topic} and the following prompt: "{sql_prompt}"
    columns_to_list_in_prompt: [domain, topic, sql_prompt]
    llm_type: code

  - name: sql
    generation_prompt: >-
        Write an SQL query to accomplish the task described by the
        following prompt: "{sql_prompt}". \n

        Assume you have access to a database as defined below:
        ```sql
        {sql_context}
        ```
    columns_to_list_in_prompt: [domain, topic]
    llm_type: code

post_processors:
    - validator: code
      settings:
        code_lang: ansi
        code_columns: [sql_context, sql]

    - evaluator: text_to_sql
      settings:
        text_column: sql_prompt
        code_column: sql
        context_column: sql_context
"""


@pytest.fixture()
def fixture_samples() -> list[dict]:
    return [
        {
            "property_type": "town-house",
            "bedrooms": 2,
            "description": "Amazing propertyd centrally located.",
        },
        {
            "property_type": "apartment",
            "bedrooms": 1,
            "description": "New build that is only a 5 minute walk from downtown.",
        },
    ]


@pytest.fixture()
def fixture_seeds_from_samples() -> dict:
    return {
        "seed_categories": [
            {
                "name": "property_condition",
                "description": "The condition of the property as new, good, fair, or poor",
                "values": ["excellent", "very good", "good", "satisfactory"],
                "quality_rank": 2,
            }
        ],
    }


@pytest.fixture()
def fixture_seeds_from_categorical_seed_column() -> dict:
    return {
        "seed_categories": [
            {
                "name": "wall_texture",
                "description": "Texture of the walls in the property",
                "values": ["orange peel", "smooth", "knock down", "popcorn"],
            }
        ],
    }


@patch("gretel_client.navigator.data_designer.interface.get_navigator_client")
def test_data_designer_entrypoint(get_navigator_client, fixture_config):
    DataDesigner.from_config(fixture_config, session=get_session_config())
    get_navigator_client.assert_called_once()


@patch("gretel_client.navigator.data_designer.interface.DataDesignerWorkflow")
def test_data_designer_reuses_workflow_for_session(dd_workflow, fixture_config):
    mock_batch_job = MagicMock(workflow_id="w_1", project_id="proj_1")
    dd_workflow.return_value.submit_batch_job.return_value = mock_batch_job

    dd_session = DataDesigner.from_config(fixture_config, session=get_session_config())
    dd_session.submit_batch_workflow(num_records=100)

    print(dd_workflow.call_args)

    dd_workflow.return_value.submit_batch_job.assert_called_with(
        num_records=100, project_name=None, workflow_id=None
    )

    dd_session.submit_batch_workflow(num_records=200)
    dd_workflow.return_value.submit_batch_job.assert_called_with(
        num_records=200, project_name="proj_1", workflow_id="w_1"
    )


@patch("gretel_client.navigator.data_designer.interface.get_navigator_client")
@patch("gretel_client.navigator.workflow.DataDesignerWorkflow.generate_preview")
def test_data_designer_s2d_run_data_seeds_step(
    mock_generate_preview,
    mock_get_navigator_client,
    fixture_samples,
    fixture_seeds_from_samples,
):
    # seeds should only be generated based on the samples provided
    mock_client = MagicMock()
    mock_s2d_preview_result = MagicMock()
    mock_s2d_preview_result.output = fixture_seeds_from_samples
    mock_generate_preview.return_value = mock_s2d_preview_result
    mock_client.registry.return_value = []
    mock_get_navigator_client.return_value = mock_client
    dd_from_samples = DataDesignerFromSampleRecords(sample_records=fixture_samples)
    seeds = dd_from_samples.run_data_seeds_step()
    mock_generate_preview.assert_called_once()
    assert len(seeds.seed_categories) == 1
    assert seeds.seed_categories[0].name == "property_condition"


@patch("gretel_client.navigator.data_designer.interface.get_navigator_client")
@patch("gretel_client.navigator.workflow.DataDesignerWorkflow.generate_preview")
def test_data_designer_s2d_run_data_seeds_step_with_categorical_seeds(
    mock_generate_preview,
    mock_get_navigator_client,
    fixture_samples,
    fixture_seeds_from_samples,
    fixture_seeds_from_categorical_seed_column,
):
    # seeds should be a union of the ones generated from samples and those from
    # seed categories added by the user manually
    mock_client = MagicMock()
    mock_s2d_preview_result = MagicMock()
    mock_s2d_preview_result.output = fixture_seeds_from_samples

    mock_cat_seed_preview_result = MagicMock()
    mock_cat_seed_preview_result.output = fixture_seeds_from_categorical_seed_column

    mock_generate_preview.side_effect = [
        mock_cat_seed_preview_result,
        mock_s2d_preview_result,
    ]
    mock_get_navigator_client.return_value = mock_client
    dd_from_samples_with_cat_seeds = DataDesignerFromSampleRecords(
        sample_records=fixture_samples
    )
    seed_category = fixture_seeds_from_categorical_seed_column["seed_categories"][0]
    dd_from_samples_with_cat_seeds.add_categorical_seed_column(
        name=seed_category["name"],
        description=seed_category["description"],
        values=seed_category["values"],
    )
    seeds = dd_from_samples_with_cat_seeds.run_data_seeds_step()
    assert mock_generate_preview.call_count == 2
    assert len(seeds.seed_categories) == 2

    assert seeds.seed_categories[0].name == "wall_texture"
    assert seeds.seed_categories[1].name == "property_condition"
