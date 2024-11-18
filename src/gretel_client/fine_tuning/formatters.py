import json
import re

from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional, Union

import numpy as np
import pandas as pd

_TEMPLATE_PARSER = re.compile(r"\{(.*?)\}")


def _write_jsonl_file(
    data: list[dict], output_file: str, encoding: Optional[str] = None
) -> None:
    with open(output_file, "w", encoding=encoding) as fout:
        for record in data:
            fout.write(json.dumps(record) + "\n")


def _convert_ndarrays(item):
    if isinstance(item, dict):
        return {k: _convert_ndarrays(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [_convert_ndarrays(i) for i in item]
    elif isinstance(item, np.ndarray):
        return item.tolist()
    return item


def _convert_dict_columns_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datasets from Huggingface often have embedded ndarrays in the columns for complex data
    types. This helper will recurse through the DataFrame and convert any ndarrays to lists.
    """
    df = df.copy()

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: (
                json.dumps(_convert_ndarrays(x)) if isinstance(x, (dict, list)) else x
            )
        )
    return df


def file_writer(encoding: Optional[str] = None):
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            result = method(self, *args, **kwargs)
            if output_file := kwargs.get("output_file"):
                _write_jsonl_file(result, output_file, encoding=encoding)
            return result

        return wrapper

    return decorator


class BaseFormatter(ABC):
    """
    Base class for formatters that format data for fine-tuning and completion datasets.
    Should not be used directly.
    """

    user_template: str
    assistant_template: str
    _expected_user_fields: set[str]
    _expected_assistant_fields: set[str]

    def __init__(self, *, user_template: str, assistant_template: str):
        self.user_template = user_template
        self.assistant_template = assistant_template
        self._expected_user_fields = set(_TEMPLATE_PARSER.findall(user_template))
        self._expected_assistant_fields = set(
            _TEMPLATE_PARSER.findall(assistant_template)
        )

    def _check_completion_record(self, record: dict) -> None:
        missing_fields = self._expected_user_fields - set(record.keys())
        if missing_fields:
            raise ValueError(
                f"Completion record does not match expected fields. "
                f"Missing fields: {missing_fields}."
            )

    def _parse_template_variables(
        self, input_dataset: pd.DataFrame
    ) -> tuple[list[str], pd.DataFrame]:
        all_variables = self._expected_user_fields | self._expected_assistant_fields

        missing_variables = all_variables - set(input_dataset.columns)
        if missing_variables:
            raise ValueError(
                f"Missing columns in input dataset: {missing_variables}. All template variables must be columns in the input dataset."
            )

        template_dataset = input_dataset[list(all_variables)]
        template_dataset = _convert_dict_columns_to_json(template_dataset)
        all_variables_list = list(all_variables)
        return all_variables_list, template_dataset

    @abstractmethod
    def create_ft_dataset(
        self, *, input_dataset: pd.DataFrame, output_file: Optional[str] = None
    ) -> list[dict]: ...

    @abstractmethod
    def create_completion_dataset(
        self,
        *,
        user_messages: Union[dict, list[dict]],
        output_file: Optional[str] = None,
    ) -> list[dict]: ...

    def peek_ft_dataset(self, *, input_dataset: pd.DataFrame, n: int = 3) -> list[dict]:
        """
        Peek at the formatted fine-tuning dataset without writing it to a file.

        Args:
            input_dataset: The input dataset containing the data to be formatted.
        """
        return self.create_ft_dataset(input_dataset=input_dataset.head(n))


class TextFormatter(BaseFormatter):
    """
    Format a DataFrame into a string for fine-tuning and/or inference.

    Args:
        user_template: Template string for user messages. All template variables must be columns in the input dataset.
        assistant_template: Template string for assistant messages. All template variables must be columns in the input dataset.
        user_header: Optional header for user messages.
        assistant_header: Optional header for assistant messages.
        column_name: Optional name for the column containing the formatted messages. Default is "text".
    """

    def __init__(
        self,
        *,
        user_header: str = "",
        assistant_header: str = "",
        column_name: str = "text",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.user_header = user_header
        self.assistant_header = assistant_header
        self.column_name = column_name

    @file_writer()
    def create_ft_dataset(
        self,
        *,
        input_dataset: pd.DataFrame,
        output_file: Optional[str] = None,
    ) -> list[dict]:
        """
        Creates a fine-tuning dataset by formatting the input dataset according to the user and assistant templates.
        This method injects the values from each row (by column name) into the templates to create the user and assistant messages.

        Args:
            input_dataset: The input dataset containing the data to be formatted.

        Returns:
            A list of dictionaries, where each dictionary contains the user and assistant messages for each row in the input dataset.
            Each dictionary only has one field, which is the `column_name` attribute of the formatter.
        """
        _, template_dataset = self._parse_template_variables(input_dataset)

        all_messages: list[dict] = []
        for _, row in template_dataset.iterrows():
            user_message = self.user_template.format(**row)
            assistant_message = self.assistant_template.format(**row)

            formatted_text = f"{self.user_header}\n{user_message}\n{self.assistant_header}\n{assistant_message}"
            # If there's no user header, remove the leading newline
            formatted_text = formatted_text.lstrip("\n")
            all_messages.append({self.column_name: formatted_text})

        return all_messages

    @file_writer()
    def create_completion_dataset(
        self,
        *,
        user_messages: Union[dict, list[dict]],
        output_file: Optional[str] = None,
    ) -> list[dict]:
        """
        Given a list of dictionaries, where each key in the dict is a template variable, create a completion dataset by formatting
        the user messages according to the user template. This method injects the values from each dictionary into the user template
        that was provided on instance creation.

        Args:
            user_messages: A list of dictionaries, where each dictionary contains the values for the template variables in the user template.
            output_file: An optional output file to write the formatted dataset to (JSONL).
        """
        if isinstance(user_messages, dict):
            user_messages = [user_messages]

        all_messages: list[dict] = []
        for user_message in user_messages:
            self._check_completion_record(user_message)
            formatted_text = f"{self.user_header}\n{self.user_template.format(**user_message)}\n{self.assistant_header}"
            formatted_text = formatted_text.lstrip("\n")
            all_messages.append({self.column_name: formatted_text})

        return all_messages


class OpenAIFormatter(BaseFormatter):
    """
    Format a DataFrame into a OpenAI chat completion datasets for fine-tuning and/or inference.

    Args:
        user_template: Template string for user messages. All template variables must be columns in the input dataset.
        assistant_template: Template string for assistant messages. All template variables must be columns in the input dataset.
        system_message: Optional system message to include in the output.
    """

    def __init__(
        self,
        *,
        system_message: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.system_message = system_message

    @file_writer(encoding="utf-8-sig")
    def create_ft_dataset(
        self,
        *,
        input_dataset: pd.DataFrame,
        output_file: Optional[str] = None,
    ) -> list[dict]:
        """
        Given a DataFrame, create an OpenAI compatible fine-tuning dataset by formatting
        the input dataset according to the user and assistant templates. If a system message
        was provided on instance creation, it will be included in the output.

        Args:
            input_dataset: The input dataset containing the data to be formatted.
            output_file: An optional output file to write the formatted dataset to (JSONL).
        """

        _, template_dataset = self._parse_template_variables(input_dataset)

        all_messages = []
        system_message_dict = {}
        if self.system_message:
            system_message_dict = {"role": "system", "content": self.system_message}

        for _, row in template_dataset.iterrows():
            this_message = []
            user_message = self.user_template.format(**row)
            assistant_message = self.assistant_template.format(**row)
            user_dict = {"role": "user", "content": user_message}
            assistant_dict = {"role": "assistant", "content": assistant_message}

            if system_message_dict:
                this_message.append(system_message_dict)

            this_message.append(user_dict)
            this_message.append(assistant_dict)
            all_messages.append({"messages": this_message})

        return all_messages

    @file_writer()
    def create_completion_dataset(
        self,
        *,
        user_messages: Union[dict, list[dict]],
        output_file: Optional[str] = None,
    ) -> list[dict]:
        """
        Given a list of dictionaries, where each key in the dict is a template variable, create an OpenAI comaptible
        completion dataset by formatting the user messages according to the user template.
        This method injects the values from each dictionary into the user template that was provided on instance creation.

        Args:
            user_messages: A list of OpenAI chat completion messages, where each dictionary contains
                the values for the template variables in the user template.
            output_file: An optional output file to write the formatted dataset to (JSONL).
        """
        if isinstance(user_messages, dict):
            user_messages = [user_messages]

        all_messages: list[dict] = []
        system_message_dict = {}
        if self.system_message:
            system_message_dict = {"role": "system", "content": self.system_message}

        for user_message in user_messages:
            self._check_completion_record(user_message)
            this_message = []
            if system_message_dict:
                this_message.append(system_message_dict)

            user_dict = {
                "role": "user",
                "content": self.user_template.format(**user_message),
            }
            this_message.append(user_dict)
            all_messages.append({"messages": this_message})

        return all_messages
