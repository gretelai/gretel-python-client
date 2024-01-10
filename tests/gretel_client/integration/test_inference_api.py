import os

import pandas as pd
import pytest

from gretel_client.inference_api.base import GretelInferenceAPIError
from gretel_client.inference_api.tabular import TabularLLMInferenceAPI

PROMPT = """\
Generate a dataset of characters from the Simpsons.

Each character should have the following columns:
* first_name: The first name of the character.
* last_name: The last name of the character.
* favorite_band: The character's all-time favorite band.
* favorite_tv_show: The character's favorite TV show other than The Simpsons.
* favorite_food: The character's favorite food.
"""

NUM_RECORDS = 5

SIMPSONS_TABLE = [
    {
        "first_name": "Homer",
        "last_name": "Simpson",
        "favorite_band": "The Rolling Stones",
        "favorite_tv_show": "Breaking Bad",
        "favorite_food": "Donuts",
    },
    {
        "first_name": "Marge",
        "last_name": "Simpson",
        "favorite_band": "The Beatles",
        "favorite_tv_show": "Friends",
        "favorite_food": "Pasta",
    },
    {
        "first_name": "Bart",
        "last_name": "Simpson",
        "favorite_band": "Nirvana",
        "favorite_tv_show": "Stranger Things",
        "favorite_food": "Pizza",
    },
    {
        "first_name": "Lisa",
        "last_name": "Simpson",
        "favorite_band": "Queen",
        "favorite_tv_show": "The Office",
        "favorite_food": "Sushi",
    },
    {
        "first_name": "Maggie",
        "last_name": "Simpson",
        "favorite_band": "Baby Einstein",
        "favorite_tv_show": "Game of Thrones",
        "favorite_food": "Ice Cream",
    },
]


@pytest.fixture(scope="module")
def tabllm():
    return TabularLLMInferenceAPI(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_tabllm_inference_api_generate(tabllm):
    df = tabllm.generate(PROMPT, num_records=NUM_RECORDS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == NUM_RECORDS


def test_tabllm_inference_api_generate_stream(tabllm):
    record_list = []
    for record in tabllm.generate(PROMPT, num_records=NUM_RECORDS, stream=True):
        assert isinstance(record, dict)
        record_list.append(record)
    assert len(record_list) == NUM_RECORDS


def test_tabllm_inference_api_edit(tabllm):
    df = tabllm.edit(
        prompt="Please add a column that describes the character's personality.",
        seed_data=SIMPSONS_TABLE,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) == 6


def test_tabllm_inference_api_edit_invalid_seed_data_type(tabllm):
    with pytest.raises(GretelInferenceAPIError):
        _ = tabllm.edit(
            prompt="Please add a column that describes the character's personality.",
            seed_data=["Eat my shorts!"],
        )
