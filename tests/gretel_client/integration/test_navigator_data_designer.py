from pathlib import Path

import pandas as pd

from gretel_client.config import get_session_config
from gretel_client.navigator import DataDesignerFactory
from gretel_client.projects import Project

config = """\
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

sample_records = [
    {
        "question": "In January the families visiting a national park see animals 26 times. In February the families that visit the national park see animals three times as many as were seen there in January. Then in March the animals are shyer and the families who visit the national park see animals half as many times as they were seen in February. How many times total did families see an animal in the first three months of the year?",
        "answer": "Animals were seen 26 times in January and three times as much in February, 26 x 3 = <<26*3=78>>78 times animals were seen in February.\nIn March the animals were seen 1/2 as many times as were seen in February, 78 / 2 = <<78/2=39>>39 times animals were seen in March.\nIf animals were seen 26 times in January + 78 times in February + 39 times in March = <<26+78+39=143>>143 times animals were seen in the first three months of the year.\n#### 143",
    },
    {
        "question": "Sarah is checking out books from the library to read on vacation. She can read 40 words per minute. The books she is checking out have 100 words per page and are 80 pages long. She will be reading for 20 hours. How many books should she check out?",
        "answer": "Each book has 8,000 words because 100 x 80 = <<100*80=8000>>8,000\nShe can finish each book in 200 minutes because 8,000 / 40 = <<8000/40=200>>200\nShe will be reading for 1,200 minutes because 20 x 60 = <<20*60=1200>>1,200\nShe needs to check out 6 books because 1,200 / 200 = <<6=6>>6\n#### 6",
    },
    {
        "question": "At the beginning of the day there were 74 apples in a basket. If Ricki removes 14 apples and Samson removes twice as many as Ricki. How many apples are left in the basket by the end of the day?",
        "answer": "There are 74-14 = <<74-14=60>>60 apples left after Ricki removes some.\nSamson removes 14*2 = <<14*2=28>>28 apples.\nThere are 60-28 = <<60-28=32>>32 apples left after Samson removes some.\n#### 32",
    },
    {
        "question": "A man drives 60 mph for 3 hours.  How fast would he have to drive over the next 2 hours to get an average speed of 70 mph?",
        "answer": "To have an average speed of 70 mph over 5 hours he needs to travel 70*5=<<70*5=350>>350 miles.\nHe drove 60*3=<<60*3=180>>180 miles in the first 3 hours\nHe needs to travel another 350-180=<<350-180=170>>170 miles over the next 2 hours.\nHis speed needs to be 170/2=<<170/2=85>>85 mph\n#### 85",
    },
    {
        "question": "Jaynie wants to make leis for the graduation party.  It will take 2 and half dozen plumeria flowers to make 1 lei.  If she wants to make 4 leis, how many plumeria flowers must she pick from the trees in her yard?",
        "answer": "To make 1 lei, Jaynie will need 2.5 x 12 = <<12*2.5=30>>30 plumeria flowers.\nTo make 4 leis, she will need to pick 30 x 4 = <<30*4=120>>120 plumeria flowers from the trees.\n#### 120",
    },
    {
        "question": "A school is buying virus protection software to cover 50 devices.  One software package costs $40 and covers up to 5 devices. The other software package costs $60 and covers up to 10 devices.  How much money, in dollars, can the school save by buying the $60 software package instead of the $40 software package?",
        "answer": "There are 50/5 = <<50/5=10>>10 sets of 5 devices in the school.\nSo the school will pay a total of $40 x 10 = $<<40*10=400>>400 for the $40 software package.\nThere are 50/10 = <<50/10=5>>5 sets of 10 devices in the school.\nSo the school will pay a total of $60 x 5 = $<<60*5=300>>300 for the $60 software package.\nThus, the school can save $400 - $100 = $<<400-100=300>>300 from buying the $60 software instead of the $40 software package.\n#### 100",
    },
    {
        "question": "Quinten sees three buildings downtown and decides to estimate their heights. He knows from a book on local buildings that the one in the middle is 100 feet tall. The one on the left looks like it is 80% of the height of the middle one. The one on the right looks 20 feet shorter than if the building on the left and middle were stacked on top of each other. How tall does Quinten estimate their total height to be?",
        "answer": "He estimates the building on the left is 80 feet tall because 100 x .8 = <<100*.8=80>>80\nThe combined height of the left and middle is 180 because 100 + 80 = <<100+80=180>>180\nThe building on the right he estimates as 160 feet because 180 - 20 = <<180-20=160>>160\nHe estimates the combined height as 340 feet because 80 + 100 + 160 = <<80+100+160=340>>340\n#### 340",
    },
    {
        "question": "At a pool party, there are 4 pizzas cut into 12 slices each.  If the guests eat 39 slices, how many slices are left?",
        "answer": "There’s a total of 4 x 12 = <<4*12=48>>48 slices.\nAfter the guests eat, there are 48 - 39 = <<48-39=9>>9 pieces.\n#### 9",
    },
    {
        "question": "A farmer gets 20 pounds of bacon on average from a pig. He sells each pound for $6 at the monthly farmer’s market. This month’s pig is a runt that grew to only half the size of the average pig. How many dollars will the farmer make from the pig’s bacon?",
        "answer": "The pig grew to half the size of the average pig, so it will produce 20 / 2 = <<20/2=10>>10 pounds of bacon.\nThe rancher will make 10 * 6 = $<<10*6=60>>60 from the pig’s bacon.\n#### 60",
    },
    {
        "question": "Legacy has 5 bars of gold she received from her father. Her friend Aleena has 2 bars fewer than she has. If a bar of gold is worth $2200, calculate the total value of gold the three have together.",
        "answer": "If Legacy has 5 bars, Aleena has 5 bars - 2 bars = <<5-2=3>>3 bars.\nIn total, they have 5 bars + 3 bars = <<5+3=8>>8 bars,\nSince one bar of gold is worth $2200, the 8 bars they have together are worth 8 bars * $2200/bar = $<<8*2200=17600>>17600\n#### 17600",
    },
]


def test_from_config_smoke_test(project: Project, tmpdir: Path):
    data_designer = DataDesignerFactory.from_config(
        config, session=get_session_config()
    )
    data_seeds = data_designer.run_data_seeds_step()
    preview = data_designer.generate_dataset_preview(data_seeds=data_seeds)

    preview_df: pd.DataFrame = preview.output

    assert len(preview_df) == 10

    preview.display_sample_record()

    batch_job = data_designer.submit_batch_workflow(
        num_records=10, project_name=project.name, data_seeds=data_seeds
    )
    df = batch_job.fetch_dataset(wait_for_completion=True)

    assert len(df) == 10

    path: Path = batch_job.download_evaluation_report(
        output_dir=tmpdir, wait_for_completion=True
    )

    assert len(path.read_bytes()) > 0


def test_from_sample_records_smoke_test(project: Project, tmpdir: Path):
    data_designer = DataDesignerFactory.from_sample_records(
        sample_records, session=get_session_config()
    )

    generation_prompt = """\
    Provide a thoughtful analysis of the quality of the answer to the provided question.

    In your analysis, consider whether the answer is accurate, relevant, and complete.
        
    *** Question ***
    {question}

    *** Answer ***
    {answer}
    """

    data_designer.add_generated_data_column(
        name="analysis", generation_prompt=generation_prompt
    )

    data_designer.add_evaluator("general")

    data_seeds = data_designer.run_data_seeds_step()
    preview = data_designer.generate_dataset_preview(data_seeds=data_seeds)

    preview_df: pd.DataFrame = preview.output

    assert len(preview_df) == 50

    preview.display_sample_record()
