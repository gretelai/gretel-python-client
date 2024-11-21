import json

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.constants import MAX_SAMPLE_SIZE
from gretel_client.navigator.tasks.types import RecordsT

logger = get_logger(__name__, level="INFO")


json_constant_map = {
    "-Infinity": float("-Infinity"),
    "Infinity": float("Infinity"),
    "NaN": None,
}


def process_sample_records(
    sample_records: Union[str, Path, pd.DataFrame, RecordsT],
    subsample_size: Optional[int] = None,
) -> RecordsT:
    if isinstance(sample_records, (str, Path)):
        sample_records = Path(sample_records)
        if sample_records.suffix == ".csv":
            sample_records = pd.read_csv(sample_records)
        elif sample_records.suffix == ".json":
            sample_records = pd.read_json(sample_records)
        else:
            raise ValueError(
                f"Unsupported file format for sample records: {sample_records.suffix}. "
                "Supported formats are .csv and .json."
            )
    elif isinstance(sample_records, list):
        sample_records = pd.DataFrame.from_records(sample_records)
    elif not isinstance(sample_records, pd.DataFrame):
        raise ValueError(
            "Sample records must be a DataFrame, list of records, or a path to a CSV or JSON file."
        )

    sample_size = len(sample_records)

    if sample_size > MAX_SAMPLE_SIZE and (
        subsample_size is None or subsample_size > MAX_SAMPLE_SIZE
    ):
        raise ValueError(
            f"The sample size of {sample_size} records is larger than the "
            f"maximum allowed size of {MAX_SAMPLE_SIZE}. Consider setting "
            f"subsample_size <= {MAX_SAMPLE_SIZE} to reduce the sample size."
        )

    if subsample_size is not None:
        if subsample_size < 1:
            raise ValueError("Subsample size must be at least 1.")

        elif subsample_size > MAX_SAMPLE_SIZE:
            logger.warning(
                f"⚠️ The given subsample size of {subsample_size} is larger than both the input "
                f"sample size and the maximum allowed size of {MAX_SAMPLE_SIZE}. We will shuffle "
                f"the input data and use the full sample size of {len(sample_records)} records."
            )

        elif subsample_size > sample_size:
            logger.warning(
                f"⚠️ The given subsample size of {subsample_size} is larger than the number of "
                f"records in the sample data. We will shuffle the data and use the "
                f"full sample size of {len(sample_records)} records."
            )

        else:
            logger.info(
                f"🎲 Randomly sampling {subsample_size} records from the input data."
            )
            sample_size = subsample_size

    sample_records = (
        sample_records.sample(sample_size, replace=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )

    # Convert NaN and Infinity values to JSON serializable values.
    sample_records = [
        json.loads(json.dumps(record), parse_constant=lambda c: json_constant_map[c])
        for record in sample_records
    ]
    return sample_records
