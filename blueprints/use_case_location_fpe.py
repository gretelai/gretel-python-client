"""
Use case location encryption and decryption based on a persons consent.
"""
from gretel_client.transformers import SecureFpeConfig, ConditionalConfig, RedactWithLabelConfig
from gretel_client.transformers import DataPath, DataTransformPipeline, FieldRef, DataRestorePipeline
from tests.src.gretel_client.unit.transformers.conftest import records_conditional

xf_fpe = SecureFpeConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                         float_precision=2)
xf_consent = ConditionalConfig(conditional_value=FieldRef('user_consent'), regex=r"['1']",
                               true_xform=xf_fpe,
                               false_xform=RedactWithLabelConfig())

data_paths_encrypt = [DataPath(input='lon', xforms=xf_fpe),
                      DataPath(input='lat', xforms=xf_fpe),
                      DataPath(input='*')
                      ]

data_paths_decrypt = [DataPath(input='lon', xforms=xf_consent),
                      DataPath(input='lat', xforms=xf_consent),
                      DataPath(input='*')
                      ]

xf_encrypt = DataTransformPipeline(data_paths_encrypt)
xf_decrypt = DataRestorePipeline(data_paths_decrypt)
check_aw = xf_encrypt.transform_record(records_conditional[0])
check_ae = xf_encrypt.transform_record(records_conditional[1])

xf = [SecureFpeConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                      float_precision=2)]

paths = [
    DataPath(input="lat", xforms=xf),
    DataPath(input="lon", xforms=xf),
    DataPath(input="name", xforms=xf2),
    DataPath(input="*")
]

pipe = DataTransformPipeline(paths)

records = [
    {
        'name': 'John Doe',
        'user_id': '0003',
        'address': '123 Fantasy Street, Awesome Town, CA 91234',
        'lat': 112.22134,
        'lon': 135.76433,
        'user_consent': '1'
    },
    {
        'name': 'Jane Hancock',
        'user_id': '0063',
        'address': '123 University Center Ln., San Diego, CA 92121',
        'lat': 32.870900,
        'lon': -117.226800,
        'user_consent': '0'
    },
    {
        'name': 'Beavis Smith',
        'user_id': '0078',
        'address': '123 Operation Blvd, San Diego, CA 92121',
        'lat': 32.880088,
        'lon': -117.198943,
        'user_consent': '1'
    },
]
rec = {
    "name": "John Doe",
    "credit_card": "4123 5678 9012 3456"
}

out = pipe.transform_record(rec)

assert out == {
    "name": "2DZv ZmN",
    "credit_card": "0667 0781 9899 8041"
}

print(out)
