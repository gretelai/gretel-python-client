import pytest
from faker import Faker
from gretel_client.transformers.transformers.fake_constant import VALID_LABELS
from gretel_client.transformers.base import factory
from gretel_client.transformers.fakers import _Fakers, FAKER_MAP
from gretel_client.transformers import FakeConstantConfig


def test_fake_constant():
    test_values_list = [
        (["tr-TR"], "name", "John Doe", "Jane Doe", "Erensoy Karadeniz"),
        (["tr-TR"], "first_name", "John", "Frank", "Fatih"),
        (["tr-TR"], "email", "test@email.com", "test@yahoo.com", "banu87@gmail.com"),
        (["en-US"], "email", "test@email.com", "test@yahoo.com", "colleen87@gmail.com"),
        (["en-US"], "first_name", "John", "Frank", "James"),
        (["en-US"], "last_name", "Johnson", "Smith", "Huang"),
        (["en-US"], "name", "John Doe", "Jane Doe", "Eduardo Walter"),
        (["es-MX"], "email", "test@email.com", "test@yahoo.com", "luisa87@gmail.com"),
        (["es-MX"], "first_name", "John", "Frank", "Catalina"),
        (["es-MX"], "last_name", "Johnson", "Smith", "Tovar"),
        (["es-MX"], "name", "John Doe", "Jane Doe", "Elvia Rosario Almonte Luna"),
    ]
    for test_values in test_values_list:
        fake_config = FakeConstantConfig(
            locales=test_values[0], seed=12345, fake_method=test_values[1]
        )
        xf = factory(fake_config)
        encode1 = xf.transform_field(test_values[1], test_values[2], None)
        encode2 = xf.transform_field(test_values[1], test_values[2], None)
        encode3 = xf.transform_field(test_values[1], test_values[3], None)
        assert encode1 == {test_values[1]: test_values[4]}
        assert encode2 == encode1 != encode3


def test_faker_map():
    faker_lib = Faker()
    fakers = _Fakers(seed=54321, locales=None, locale_seed=0)
    new_fake_value = {}
    num_fakers = sum(1 for _ in filter(None.__ne__, FAKER_MAP.values()))
    for key, val in FAKER_MAP.items():
        if val:
            func = getattr(faker_lib, val)
            fake_val = func()
            if fake_val:
                new_fake_value[key] = fakers.constant_fake(fake_val, val)
    assert len(new_fake_value) == num_fakers


def test_faker_locales():
    locales = ["tr-TR", "es-MX", "en-US", "de-DE"]
    fake_config = FakeConstantConfig(locales=locales, seed=12345, fake_method="name")
    xf = factory(fake_config)
    name_set = set()
    while len(name_set) < len(locales):
        record = xf.transform_field("person_name", "John Doe", None)
        name_set.add(record["person_name"])


def test_faker_locales_seed_randomize():
    name_set = set()
    locales = ["tr-TR", "es-MX"]
    for _ in range(10):
        fake_config = FakeConstantConfig(
            locales=locales, seed=12345, fake_method="name", locale_seed=None
        )
        xf = factory(fake_config)
        record = xf.transform_field("person_name", "John Doe", None)
        name_set.add(record["person_name"])
    assert len(name_set) > 1


def test_faker_locales_seed_deterministc():
    name_set = set()
    locales = ["tr-TR", "es-MX"]
    for i in range(10):
        fake_config = FakeConstantConfig(
            locales=locales, seed=12345, fake_method="name"
        )
        xf = factory(fake_config)
        record = xf.transform_field("person_name", "John Doe", None)
        name_set.add(record["person_name"])
    assert len(name_set) == 1


def test_faker_valid_label():
    with pytest.raises(ValueError):
        fake_config = FakeConstantConfig(seed=12345, labels=VALID_LABELS[0])
        factory(fake_config)


def test_faker_no_method():
    with pytest.raises(ValueError):
        fake_config = FakeConstantConfig(seed=12345)
        factory(fake_config)


def test_faker_invalid_label():
    with pytest.raises(ValueError):
        fake_config = FakeConstantConfig(seed=12345, labels=["fail_me"])
        factory(fake_config)
