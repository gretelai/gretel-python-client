"""
Use case location encryption and decryption based on a persons consent.
"""
from gretel_client.transformers import (
    ConditionalConfig,
    FpeFloatConfig,
    RedactWithCharConfig,
)
from gretel_client.transformers import (
    DataPath,
    DataTransformPipeline,
    FieldRef,
    DataRestorePipeline,
)

records_conditional = [
    {
        "name": "John Doe",
        "user_id": "0003",
        "address": "123 Fantasy Street, Awesome Town, CA 91234",
        "lat": 112.22134,
        "lon": 135.76433,
        "user_consent": "1",
    },
    {
        "name": "Jane Hancock",
        "user_id": "0063",
        "address": "123 University Center Ln., San Diego, CA 92121",
        "lat": 32.870900,
        "lon": -117.226800,
        "user_consent": "0",
    },
    {
        "name": "Beavis Smith",
        "user_id": "0078",
        "address": "123 Operation Blvd, San Diego, CA 92121",
        "lat": 32.880088,
        "lon": -117.198943,
        "user_consent": "1",
    },
]

test = [
    {
        "address": "123 Fantasy Street, Awesome Town, CA 91234",
        "lat": 112.22134,
        "lon": 135.76433,
        "name": "John Doe",
        "user_consent": "1",
        "user_id": "0003",
    },
    {
        "address": "123 University Center Ln., San Diego, CA 92121",
        "lat": 0.0,
        "lon": -0.0,
        "name": "Jane Hancock",
        "user_consent": "0",
        "user_id": "0063",
    },
    {
        "address": "123 Operation Blvd, San Diego, CA 92121",
        "lat": 32.880088,
        "lon": -117.198943,
        "name": "Beavis Smith",
        "user_consent": "1",
        "user_id": "0078",
    },
]

xf_fpe = FpeFloatConfig(
    secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
    radix=10,
    float_precision=2,
)
xf_consent = ConditionalConfig(
    conditional_value=FieldRef("user_consent"),
    regex=r"['1']",
    true_xform=xf_fpe,
    false_xform=RedactWithCharConfig(char="0"),
)

data_paths_encrypt = [
    DataPath(input="lon", xforms=xf_fpe),
    DataPath(input="lat", xforms=xf_fpe),
    DataPath(input="*"),
]

data_paths_decrypt = [
    DataPath(input="lon", xforms=xf_consent),
    DataPath(input="lat", xforms=xf_consent),
    DataPath(input="*"),
]

xf_encrypt = DataTransformPipeline(data_paths_encrypt)
xf_decrypt = DataRestorePipeline(data_paths_decrypt)
for record, test in zip(records_conditional, test):
    encrypted = xf_encrypt.transform_record(record)
    decrypted = xf_decrypt.transform_record(encrypted)
    assert decrypted == test
