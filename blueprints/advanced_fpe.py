"""
Basic Format Preserving Encryption
"""
from gretel_client.transformers import SecureFpeConfig
from gretel_client.transformers import DataPath, DataTransformPipeline, DataRestorePipeline

xf = [SecureFpeConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                      mask="1000 0000 0000 0000", radix=10)]
xf2 = [SecureFpeConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=62)]

paths = [
    DataPath(input="l", xforms=xf),
    DataPath(input="name", xforms=xf2),
    DataPath(input="*")
]

transform_pipe = DataTransformPipeline(paths)
restore_pipe = DataRestorePipeline(paths)

records = [
    {
        "foo": "hello",
        "name": "adam yauch",
        "score": 1.234
    },
    {
        "name": "jerome horowitz",
        "score": 2.234
    },
    {
        "foo": "hello",
        "name": "quincy jones",
        "score": 3.234
    },
    {
        "name": "zeppo marx",
        "score": -1.234
    },
]


out = transform_pipe.transform_record(rec)

assert out == {
    "name": "2DZv ZmN",
    "credit_card": "4521 1021 2994 9272"
}

print(out)

restored = restore_pipe.transform_record(out)

assert rec == restored

print(restored)
