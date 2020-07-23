"""
Basic Format Preserving Encryption
"""
from gretel_client.transformers import FpeStringConfig
from gretel_client.transformers import DataPath, DataTransformPipeline
from gretel_client.transformers.string_mask import StringMask

mask = StringMask(start_pos=1)

xf = FpeStringConfig(
    secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
    radix=10,
    mask=[mask],
)
xf2 = FpeStringConfig(
    secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=62
)

paths = [
    DataPath(input="credit_card", xforms=xf),
    DataPath(input="name", xforms=xf2),
    DataPath(input="*"),
]

pipe = DataTransformPipeline(paths)

rec = {"name": "John Doe", "credit_card": "4123 5678 9012 3456"}

out = pipe.transform_record(rec)

assert out == {"name": "2DZv ZmN", "credit_card": "4521 1021 2994 9272"}

print(out)
