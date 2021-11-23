from pathlib import Path

import pytest

from gretel_client.projects.common import validate_data_source
from gretel_client.projects.exceptions import DataSourceError, DataValidationError

ok_csv = """header_1,header_2
1,2
"""

bad_csv = """,
"""

ok_json = """{'test': 1}
{'tests': 2}
"""

bad_json = """{'test': true
"""


def test_valid_ok_file(tmp_path: Path):
    t = tmp_path / "data.csv"
    t.write_text(ok_csv)
    assert validate_data_source(t)


def test_fail_file_not_found():
    with pytest.raises(DataSourceError):
        validate_data_source("path/not_found")


@pytest.mark.parametrize("file_type,bad_data", [("csv", bad_csv), ("json", bad_json)])
def test_fail_invalid_data(file_type: str, bad_data: str, tmp_path: Path):
    f = tmp_path / f"data.{file_type}"
    f.write_text(bad_data)
    with pytest.raises(DataValidationError) as ex:
        validate_data_source(f)
        assert str(f) in str(ex)


@pytest.mark.parametrize("file_type,ok_data", [("csv", ok_csv), ("json", ok_json)])
def test_ok_data(file_type: str, ok_data: str, tmp_path: Path):
    f = tmp_path / f"data.{file_type}"
    f.write_text(ok_data)
    validate_data_source(f)
