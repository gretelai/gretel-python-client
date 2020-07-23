from gretel_client.transformers import FpeStringConfig
from gretel_client.transformers.base import factory
from gretel_client.transformers.string_mask import StringMask


def test_fpe_string():
    mask_last_name = StringMask(mask_after=' ')
    mask_first_name = StringMask(mask_until=' ')
    fpe_string_config = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                                        radix=62, mask=[mask_last_name])
    xf = factory(fpe_string_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'John BDy'}
    record = xf._restore_field('person_name', record['person_name'], None)
    assert record == {'person_name': 'John Doe'}

    fpe_string_config = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                                        radix=62, mask=[mask_first_name])
    xf = factory(fpe_string_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'Uugx Doe'}
    record = xf._restore_field('person_name', record['person_name'], None)
    assert record == {'person_name': 'John Doe'}

    fpe_string_config = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                                        radix=62, mask=[mask_first_name, mask_last_name])
    xf = factory(fpe_string_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'Uugx BDy'}
    record = xf._restore_field('person_name', record['person_name'], None)
    assert record == {'person_name': 'John Doe'}
