import os

import pandas as pd
import pytest

from gretel_client.inference_api.base import GretelInferenceAPIError
from gretel_client.inference_api.natural_language import NaturalLanguageInferenceAPI
from gretel_client.inference_api.tabular import TabularInferenceAPI

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

SIMPSONS_TABLE_DF = pd.DataFrame(SIMPSONS_TABLE)


@pytest.fixture(scope="module")
def llm():
    return NaturalLanguageInferenceAPI(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


@pytest.fixture(scope="module")
def nav():
    return TabularInferenceAPI(
        api_key=os.getenv("GRETEL_API_KEY"), endpoint="https://api-dev.gretel.cloud"
    )


def test_llm_inference_api_generate(llm):
    response = llm.generate(
        prompt="What is the meaning of life?",
        temperature=0.1,
        max_tokens=10,
        top_k=40,
        top_p=0.9,
    )
    assert isinstance(response, str)


def test_nav_inference_api_generate(nav):
    df = nav.generate(PROMPT, num_records=NUM_RECORDS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == NUM_RECORDS


def test_nav_inference_api_generate_stream(nav):
    record_list = []
    for record in nav.generate(PROMPT, num_records=NUM_RECORDS, stream=True):
        assert isinstance(record, dict)
        record_list.append(record)
    assert len(record_list) == NUM_RECORDS


@pytest.mark.parametrize(
    "chunk_size,seed_data", [(10, SIMPSONS_TABLE), (1, SIMPSONS_TABLE_DF)]
)
def test_nav_inference_api_edit(chunk_size, seed_data, nav):
    """
    We test a chunk size that fits the entire table and a chunks size
    that requires multiple upstream calls to process the table and we
    also alternate between dict and DF as seed data

    NOTE: The prompt is very strict about the column name because
    when we make multiple upstream requests there is a chance that
    different inferences will add slightly different column names
    each time.
    """
    df = nav.edit(
        prompt="Please add exactly one and only one column called 'personality' that describes the character's personality.",
        seed_data=seed_data,
        chunk_size=chunk_size,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(SIMPSONS_TABLE)
    assert len(df.columns) == 6


def test_nav_inference_api_edit_stream(nav):
    """
    We test a chunk size that fits the entire table and a chunks size
    that requires multiple upstream calls to process the table
    """
    results = list(
        nav.edit(
            prompt="Please add a column that describes the character's personality.",
            seed_data=SIMPSONS_TABLE,
            stream=True,
        )
    )
    assert len(results) == len(SIMPSONS_TABLE)
    assert len(results[0].keys()) == 6


def test_nav_inference_api_invalid_backend_model():
    with pytest.raises(GretelInferenceAPIError):
        TabularInferenceAPI(backend_model="invalid_model")


def test_nav_inference_api_edit_invalid_seed_data_type(nav):
    with pytest.raises(GretelInferenceAPIError):
        nav.edit(
            prompt="Please add a column that describes the character's personality.",
            seed_data=["Eat my shorts!"],
        )
