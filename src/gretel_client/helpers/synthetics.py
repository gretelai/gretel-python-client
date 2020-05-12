"""
This module contains various helper functions that interact with the
Gretel API and manipulate data for easier use of the Gretel Synthetics
library.
"""
from typing import List
import pandas as pd

from tqdm.auto import tqdm

from gretel_client.projects import Project


def filter_records(
    field_meta: List[dict], max_unique_pct: float = 80.0, max_missing_pct: float = 20.0
) -> List[str]:
    """
    Returns list of field names based on statistical properties.

    Args:
        field_meta: A list of field metadata as returned from the Gretel API
        max_unique_pct: The upper bound of how many fields can be missing
        max_unique_pct: The upper bound of how many unique values a field has

    Returns:
        A list of field names
    """
    df = pd.DataFrame(field_meta)
    df = df.loc[df["pct_relative_unique"] <= max_unique_pct]
    df = df.loc[df["pct_missing"] <= max_missing_pct]
    keep_fields = list(df["field"])
    return keep_fields


def _collect_records(project: Project, max_size: int) -> List[dict]:  # pragma: no cover
    out = []
    t = tqdm(total=max_size, desc="Downloading records")
    for data in project.iter_records(
        direction="backward",
        record_limit=max_size,
        wait_for=300,  # This is probably agressive, but whatevs
    ):
        t.update(1)
        out.append(data["record"])
    t.close()
    return out


def build_training_set(
    project: Project, max_size: int, fields: List[str] = None, save_to: str = None
) -> pd.DataFrame:
    """
    Create a training set for Gretel Synthetics

    Args:
        project: A gretel-client ``Project`` instance
        size: The max number of records to use
        fields: An optional list of fields to only include
        save_to: An optional filepath where a CSV of the dataset will be
            saved to before returning the DataFrame

    Returns:
        A Pandas DataFrame with headers removed and columns in the order
        specified by the ``fields`` param if provided.
    """
    records = _collect_records(project, max_size)
    df = pd.DataFrame(records)
    df = df.fillna("")
    df = df.replace(",", "[c]", regex=True)

    if fields is not None:
        df = df[fields]

    if save_to:
        df.to_csv(save_to, index=False, header=False, sep=",")

    return df
