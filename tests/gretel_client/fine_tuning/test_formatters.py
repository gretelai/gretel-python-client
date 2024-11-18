import json
import tempfile

import numpy as np
import pandas as pd
import pytest

from openai.types.chat.chat_completion_assistant_message_param import (
    ChatCompletionAssistantMessageParam,
)
from openai.types.chat.chat_completion_system_message_param import (
    ChatCompletionSystemMessageParam,
)
from openai.types.chat.chat_completion_user_message_param import (
    ChatCompletionUserMessageParam,
)

from gretel_client.fine_tuning.formatters import (
    _convert_dict_columns_to_json,
    OpenAIFormatter,
    TextFormatter,
)

_DATASET = pd.DataFrame(
    {
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "job_title": ["Engineer", "Doctor", "Artist", "Lawyer", "Scientist"],
        "job_description": [
            "Designs and builds software.",
            "Treats patients and prescribes medication.",
            "Creates visual art and illustrations.",
            "Provides legal advice and representation.",
            "Conducts scientific research and experiments.",
        ],
    }
)


def test_text_formatter():
    # Test with no user/assistant headers
    formatter = TextFormatter(
        user_template="Write a job description for {name} who is a {job_title}:",
        assistant_template="{job_description}",
    )
    data = formatter.create_ft_dataset(input_dataset=_DATASET)
    assert data[0] == {
        "text": "Write a job description for Alice who is a Engineer:\n\nDesigns and builds software."
    }
    assert len(data) == len(_DATASET)
    data = formatter.create_completion_dataset(
        user_messages=[{"name": "Zoro", "job_title": "Pirate"}]
    )
    assert data[0] == {"text": "Write a job description for Zoro who is a Pirate:\n"}

    # Test with custom column name
    formatter = TextFormatter(
        user_template="Write a job description for {name} who is a {job_title}:",
        assistant_template="{job_description}",
        column_name="fine_tune_me",
    )
    data = formatter.create_ft_dataset(input_dataset=_DATASET)
    assert data[0] == {
        "fine_tune_me": "Write a job description for Alice who is a Engineer:\n\nDesigns and builds software."
    }

    # Test with template variables that are not in the dataset
    formatter = TextFormatter(
        user_template="Write a job description for {name} who is a {job_title}:",
        assistant_template="{job_description} {unknown_variable}",
    )
    with pytest.raises(ValueError):
        formatter.create_ft_dataset(input_dataset=_DATASET)

    # Test with user/assistant headers
    formatter = TextFormatter(
        user_template="Write a job description for {name} who is a {job_title}:",
        assistant_template="{job_description}",
        user_header="### User",
        assistant_header="### Assistant",
    )
    data = formatter.create_ft_dataset(input_dataset=_DATASET)
    assert data[0] == {
        "text": "### User\nWrite a job description for Alice who is a Engineer:\n### Assistant\nDesigns and builds software."
    }

    data = formatter.create_completion_dataset(
        user_messages=[{"name": "Zoro", "job_title": "Pirate"}]
    )
    assert data[0] == {
        "text": "### User\nWrite a job description for Zoro who is a Pirate:\n### Assistant"
    }


def _validate_oai_messages(messages: list[dict]):
    # Overkill for validating the schema? Not sure but otherwise kind of feels
    # like we're comparing the same thing to itself
    for message in messages:
        if message["role"] == "system":
            assert ChatCompletionSystemMessageParam(**message)
        elif message["role"] == "user":
            assert ChatCompletionUserMessageParam(**message)
        else:
            assert ChatCompletionAssistantMessageParam(**message)


def test_openai_formatter():
    with tempfile.NamedTemporaryFile("w") as temp_file:
        formatter = OpenAIFormatter(
            system_message="you write great!",
            user_template="Write a job description for {name} who is a {job_title}.",
            assistant_template="{job_description}",
        )
        data = formatter.create_ft_dataset(
            input_dataset=_DATASET, output_file=temp_file.name
        )
        messages = data[0]["messages"]
        _validate_oai_messages(messages)

        # make sure we have some sweet JSONL now
        count = 0
        with open(temp_file.name, encoding="utf-8-sig") as fin:
            for line in fin:
                data = json.loads(line)
                _validate_oai_messages(data["messages"])
                count += 1

        assert count == len(_DATASET)

        data = formatter.create_completion_dataset(
            user_messages={"name": "Zoro", "job_title": "Pirate"}
        )
        messages = data[0]["messages"]
        _validate_oai_messages(messages)


def test_convert_dict_columns_to_json():
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "attributes": [
                {"height": np.array([5.5, 6.0]), "weight": np.array([130, 150])},
                {"height": np.array([5.8, 6.2]), "weight": np.array([160, 180])},
            ],
        }
    )

    df = _convert_dict_columns_to_json(df)
    assert df["attributes"][0] == '{"height": [5.5, 6.0], "weight": [130, 150]}'
