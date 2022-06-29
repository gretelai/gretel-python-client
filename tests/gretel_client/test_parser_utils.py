import pandas as pd
import pytest

from gretel_client.cli.utils.parser_utils import RefData, RefDataError


@pytest.mark.parametrize(
    "input,expect,values,cli_str",
    [
        (
            ["foo", "bar"],
            RefData({0: "foo", 1: "bar"}),
            ["foo", "bar"],
            ["--ref-data", "foo", "--ref-data", "bar"],
        ),
        (
            ["foo=foo.csv", "bar=bar.csv"],
            RefData({"foo": "foo.csv", "bar": "bar.csv"}),
            ["foo.csv", "bar.csv"],
            ["--ref-data", "foo=foo.csv", "--ref-data", "bar=bar.csv"],
        ),
        (
            ["foo", "bar=bar.csv"],
            RefData({0: "foo", "bar": "bar.csv"}),
            ["foo", "bar.csv"],
            ["--ref-data", "foo", "--ref-data", "bar=bar.csv"],
        ),
    ],
)
def test_ref_data(input, expect, values, cli_str):
    ref_data = RefData.from_list(input)
    assert ref_data.ref_dict == expect.ref_dict
    assert ref_data.values == values
    assert not ref_data.is_empty
    assert ref_data.as_cli == cli_str


@pytest.mark.parametrize(
    "input,values,cli_str",
    [
        (
            [pd.DataFrame([{"key1": "value1", "key2": "value2"}])],
            ["value1", "value2"],
            ["--ref-data", pd.DataFrame([{"key1": "value1", "key2": "value2"}])],
        )
    ],
)
def test_dataframe_ref_data(input, values, cli_str):
    ref_data = RefData.from_dataframes(input)
    assert (ref_data.ref_dict[0].values == values).all()
    assert ref_data.ref_dict[0]["key1"].item() == "value1"
    assert ref_data.ref_dict[0]["key2"].item() == "value2"
    assert not ref_data.is_empty
    assert not ref_data.is_cloud_data
    assert ref_data.as_cli[0] == cli_str[0]
    assert ref_data.as_cli[1].equals(cli_str[1])


def test_ref_data_error():
    with pytest.raises(RefDataError):
        RefData.from_list(["foo=bar=baz"])


def test_empty_ref_data():
    ref_data = RefData.from_list([])
    assert ref_data.is_empty
    assert not ref_data.is_cloud_data


def test_is_cloud_data():
    ref_data = RefData.from_list(["gretel_abc"])
    assert ref_data.is_cloud_data
