import datetime

from gretel_client.transformers import DateShiftConfig
from gretel_client.transformers.base import FieldRef, factory


def test_date_shift():
    test_date = str(datetime.date(2006, 6, 15))
    date_config = DateShiftConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        lower_range_days=-10,
        upper_range_days=45,
        labels=["date"],
    )

    date_config_tweak = DateShiftConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        lower_range_days=-10,
        upper_range_days=45,
        labels=["date"],
        tweak=FieldRef("test_id"),
    )

    date2_config = DateShiftConfig(
        secret="1628AED2A6A809CBF7158F7F036D6F059D8D54FC6A942B7E15F4F3CEF4380AA4",
        lower_range_days=-10,
        upper_range_days=45,
        labels=["date"],
    )

    date2_config_tweak = DateShiftConfig(
        secret="1628AED2A6A809CBF7158F7F036D6F059D8D54FC6A942B7E15F4F3CEF4380AA4",
        lower_range_days=-10,
        upper_range_days=45,
        labels=["date"],
    )

    xf = factory(date_config)
    encode = xf.transform_field("date", test_date, None)
    decode = xf.restore_field("date", encode["date"], None)
    xf = factory(date2_config)
    encode2 = xf.transform_field("date", test_date, None)
    decode2 = xf.restore_field("date", encode2["date"], None)
    xf = factory(date_config_tweak)
    xf.field_ref_dict["tweak"] = FieldRef("user_id", 10, 17)
    encode_t = xf.transform_field("date", test_date, None)
    decode_t = xf.restore_field("date", encode_t["date"], None)
    xf = factory(date2_config_tweak)
    xf.field_ref_dict["tweak"] = FieldRef("user_id", 10, 17)
    encode2_t = xf.transform_field("date", test_date, None)
    decode2_t = xf.restore_field("date", encode2_t["date"], None)
    assert decode_t["date"] == test_date
    assert decode2_t["date"] == test_date
    assert decode["date"] == test_date
    assert decode2["date"] == test_date
    assert encode_t["date"] != test_date
    assert encode2_t["date"] != test_date
    assert encode["date"] != test_date
    assert encode2["date"] != test_date
    assert encode_t != encode2_t and encode_t != encode and encode_t != encode2
    assert encode2_t != encode and encode2_t != encode2
    assert encode != encode2
