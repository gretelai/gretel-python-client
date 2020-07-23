import jsonpickle

from gretel_client.transformers import FakeConstantConfig, RedactWithCharConfig, RedactWithLabelConfig, SecureHashConfig

SEED = 8675309


def test_config_serialize():
    # empty transformer
    xf_list = [
        # replace names with PERSON_NAM
        RedactWithLabelConfig(labels=['person_name']),

        # swap emails with fake (but consistent emails)
        FakeConstantConfig(labels=['email_address'], seed=SEED),

        # character-redact IP addresses
        RedactWithCharConfig(labels=['ip_address']),

        # field redact entire city
        RedactWithCharConfig(char='Y'),

        # this should not be run
        RedactWithCharConfig(char='N', labels=['location_city']),

        # secure hash
        SecureHashConfig(secret='rockybalboa', labels=['location_state']),

        # replace latitude
        FakeConstantConfig(labels=['latitude'], seed=SEED)
    ]

    json_encode = jsonpickle.encode(xf_list)
    loaded_xf_list = jsonpickle.decode(json_encode)
    assert [xf for xf in xf_list] == [xf for xf in loaded_xf_list]
