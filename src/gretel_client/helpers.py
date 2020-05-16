"""
This module contains various helper functions that interact with the
Gretel API.
"""
from __future__ import annotations
from typing import List, TYPE_CHECKING

from smart_open import open as smart_open

from gretel_client.projects import Project

try:
    from tqdm.auto import tqdm
except ImportError:
    tqdm = None

try:
    import pandas as pd
except ImportError:
    pd = None


if TYPE_CHECKING:
    from pandas import DataFrame as _DataFrameT
else:

    class _DataFrameT:
        ...  # noqa


from gretel_client.errors import GretelDependencyError


def _collect_records(project: Project, max_size: int) -> List[dict]:  # pragma: no cover
    if not tqdm:
        raise GretelDependencyError("tqdm required for this feature")
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


def build_df_csv(
    project: Project,
    max_size: int,
    fields: List[str] = None,
    save_to: str = None,
    headers: bool = True,
) -> _DataFrameT:
    """
    Create a DataFrame from historical records. Optionally write the data as a
    CSV with or without headers.

    Args:
        project: A gretel-client ``Project`` instance
        size: The max number of records to use
        fields: An optional list of fields to only include
        save_to: An optional filepath where a CSV of the dataset will be
            saved to before returning the DataFrame
        headers: If True, keep header names, if False, remove headers.

    Returns:
        A Pandas DataFrame with headers removed and columns in the order
        specified by the ``fields`` param if provided.
    """
    if not pd:
        raise GretelDependencyError("pandas must be installed for this feature")
    if not isinstance(project, Project):
        raise AttributeError("project must be a Project instance")
    records = _collect_records(project, max_size)
    df = pd.DataFrame(records)
    df = df.fillna("")
    df = df.replace(",", "[c]", regex=True)

    if fields is not None:
        df = df[fields]

    if save_to:
        with smart_open(save_to, "w", newline="") as fp:
            df.to_csv(fp, index=False, header=headers, sep=",")

    return df
