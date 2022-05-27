"""
Helpers for parsing CLI inputs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union


class RefDataError(Exception):
    pass


RefDataTypes = Optional[Union[str, Dict[str, str], List[str], Tuple[str]]]


@dataclass
class RefData:
    """
    Interface for parsing key/value reference data from CLI/SDK inputs
    """

    CLI_PARAM = "--ref-data"

    ref_dict: Dict[Union[int, str], str] = field(default_factory=dict)

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
        if self.is_empty:
            return False

        return self.values[0].startswith("gretel_")

    @property
    def is_local_data(self) -> bool:
        """
        Return True if all of the data sources are local to disk, False otherwise
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
        be provided as key/value pairs such as `foo=bar.csv`
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


def ref_data_factory(ref_data: Optional[RefDataTypes] = None) -> RefData:
    if ref_data is None:
        ref_data_obj = RefData()
    elif isinstance(ref_data, dict):
        ref_data_obj = RefData(ref_data)
    elif isinstance(ref_data, str):
        ref_data_obj = RefData.from_list([ref_data])
    elif isinstance(ref_data, (list, tuple)):
        ref_data_obj = RefData.from_list(list(ref_data))
    else:
        raise ValueError("ref_data is not a valid str, dict, or list")

    return ref_data_obj
