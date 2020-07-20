"""
Basic Character Redaction
"""
from gretel_client.transformers.transformers import RedactWithCharConfig
from gretel_client.transformers import DataPath, DataTransformPipeline


xf = [RedactWithCharConfig()]
xf2 = [RedactWithCharConfig(char="Y")]

paths = [
    DataPath(input="foo", xforms=xf),
    DataPath(input="bar", xforms=xf2),
    DataPath(input="*")
]

pipe = DataTransformPipeline(paths)

rec = {
    "foo": "hello",
    "bar": "there",
    "baz": "world"
}

out = pipe.transform_record(rec)

assert out == {
    "foo": "XXXXX",
    "bar": "YYYYY",
    "baz": "world"
}

print(out)
