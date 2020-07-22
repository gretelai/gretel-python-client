from gretel_client.transformers import RedactWithCharConfig
from gretel_client.transformers.base import factory
from gretel_client.transformers.string_mask import StringMask


def test_redact_with_char():
    mask_last_name = StringMask(mask_after=' ')
    mask_first_name = StringMask(mask_until=' ')
    redact_with_char_config = RedactWithCharConfig(labels=['ip_address'], mask=[mask_last_name])
    xf = factory(redact_with_char_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'John XXX'}

    redact_with_char_config = RedactWithCharConfig(labels=['ip_address'], mask=[mask_first_name])
    xf = factory(redact_with_char_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'XXXX Doe'}

    redact_with_char_config = RedactWithCharConfig(labels=['ip_address'], mask=[mask_first_name, mask_last_name])
    xf = factory(redact_with_char_config)
    record = xf.transform_field("person_name", "John Doe", None)
    assert record == {'person_name': 'XXXX XXX'}
