import pytest

from gretel_client.cli.utils.parser_utils import RefData, RefDataError


@pytest.mark.parametrize(
    "input,expect,values",
    [
        (["foo", "bar"], RefData({0: "foo", 1: "bar"}), ["foo", "bar"]),
        (
            ["foo=foo.csv", "bar=bar.csv"],
            RefData({"foo": "foo.csv", "bar": "bar.csv"}),
            ["foo.csv", "bar.csv"],
        ),
        (
            ["foo", "bar=bar.csv"],
            RefData({0: "foo", "bar": "bar.csv"}),
            ["foo", "bar.csv"],
        ),
    ],
)
def test_ref_data(input, expect, values):
    ref_data = RefData.from_list(input)
    assert ref_data.ref_dict == expect.ref_dict
    assert ref_data.values == values
    assert not ref_data.is_empty


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
