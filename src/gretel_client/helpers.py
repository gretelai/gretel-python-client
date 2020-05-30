"""
This module contains various helper functions that interact with the
Gretel API.
"""
from typing import List, TYPE_CHECKING

from smart_open import open as smart_open

from gretel_client.projects import Project

try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None


if TYPE_CHECKING:  # pragma: no cover
    from pandas import DataFrame as _DataFrameT
else:

    class _DataFrameT:
        ...  # noqa


def _collect_records(project: Project, max_size: int) -> List[dict]:  # pragma: no cover
    if not tqdm:
        raise RuntimeError("tqdm required for this feature")
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
) -> "_DataFrameT":
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
    if not pd:  # pragma: no cover
        raise RuntimeError("pandas must be installed for this feature")
    if not isinstance(project, Project):  # pragma: no cover
        raise AttributeError("project must be a Project instance")
    records = _collect_records(project, max_size)
    df = pd.DataFrame(records)

    if fields is not None:
        df = df[fields]

    if save_to:
        with smart_open(save_to, "w", newline="") as fp:
            df.to_csv(fp, index=False, header=headers, sep=",")

    return df
