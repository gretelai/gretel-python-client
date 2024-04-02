"""
Helpers for parsing CLI inputs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from gretel_client.dataframe import _DataFrameT


class RefDataError(Exception):
    pass


RefDataTypes = Optional[
    Union[str, Dict[str, str], List[str], Tuple[str], _DataFrameT, List[_DataFrameT]]
]

DataSourceTypes = Union[str, _DataFrameT]


@dataclass
class RefData:
    """
    Interface for parsing key/value reference data from CLI/SDK inputs.
    """

    CLI_PARAM = "--ref-data"

    ref_dict: Dict[Union[int, str], Union[str, _DataFrameT]] = field(
        default_factory=dict
    )

    @property
    def values(self) -> List[str]:
        return list(self.ref_dict.values())

    @property
    def is_empty(self) -> bool:
        return False if self.ref_dict else True

    @property
    def is_cloud_data(self) -> bool:
        """
        Return True if the ref data are artifacts in Gretel Cloud. We assume all
        or nothing here, so only check the first data source.
        """
        if self.is_empty or isinstance(self.values[0], _DataFrameT):
            return False

        return self.values[0].startswith("gretel_")

    @property
    def is_local_data(self) -> bool:
        """
        Return True if all of the data sources are local to disk, False otherwise.
        """
        if self.is_empty:
            return False

        for data_source in self.values:
            if not Path(data_source).is_file():
                return False

        return True

    @classmethod
    def from_list(cls, ref_data: List[str]) -> RefData:
        """
        Alternate constructor for creating an instance from CLI inputs that may
        be provided as key/value pairs such as `foo=bar.csv`.
        """
        ref_data_dict = {}

        for idx, ref in enumerate(ref_data):
            parts = ref.split("=")

            # There should only be a single '=' if at all in a given data ref
            if len(parts) > 2 and "=" in ref:
                raise RefDataError("Ref data filename may not contain a '=' char!")

            if len(parts) == 1:
                ref_data_dict[idx] = ref
            else:
                ref_data_dict[parts[0]] = parts[1]

        return cls(ref_data_dict)

    @classmethod
    def from_dataframes(cls, ref_data: List[_DataFrameT]) -> RefData:
        """
        Alternate constructor for creating an instance from the list of dataframes input.
        """
        ref_data_dict = {}
        for idx, ref in enumerate(ref_data):
            ref_data_dict[idx] = ref
        return cls(ref_data_dict)

    @property
    def as_cli(self) -> Optional[List[str]]:
        """
        Take the ref data dict and put it into CLI params.
        """
        if not self.ref_dict:
            return None

        parts = []
        for key, source in self.ref_dict.items():
            parts.append(self.CLI_PARAM)
            if isinstance(key, int):
                parts.append(source)
            else:
                parts.append(f"{key}={source}")

        return parts


def ref_data_factory(
    ref_data: Optional[Union[RefDataTypes, RefData]] = None
) -> RefData:
    if ref_data is None:
        return RefData()
    if isinstance(ref_data, RefData):
        return ref_data
    if isinstance(ref_data, dict):
        return RefData(ref_data)
    if isinstance(ref_data, _DataFrameT):
        return RefData.from_dataframes([ref_data])
    if isinstance(ref_data, str):
        return RefData.from_list([ref_data])
    if isinstance(ref_data, (list, tuple)):
        if len(ref_data) == 0:
            return RefData()
        elif isinstance(ref_data[0], _DataFrameT):
            return RefData.from_dataframes(ref_data)
        return RefData.from_list(list(ref_data))

    raise ValueError("ref_data is not a valid str, dataframe, dict, or list.")
