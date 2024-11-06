from pathlib import Path

import pandas as pd

from gretel_client.navigator import DataDesigner
from gretel_client.projects import Project

config = """\
model_suite: Apache-2.0

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


def test_basic_smoke_test(project: Project, tmpdir: Path):
    data_designer = DataDesigner.from_config(config)
    data_seeds = data_designer.generate_seed_category_values()
    preview = data_designer.generate_dataset_preview(data_seeds=data_seeds)

    preview_df: pd.DataFrame = preview.output

    assert len(preview_df) == 10

    batch_job = data_designer.submit_batch_workflow(
        num_records=10, project_name=project.name, data_seeds=data_seeds
    )
    df = batch_job.fetch_dataset(wait_for_completion=True)

    assert len(df) == 10

    path: Path = batch_job.download_evaluation_report(output_dir=tmpdir)

    assert len(path.read_bytes()) > 0
