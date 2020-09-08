"""
Basic Character Redaction
"""
import sys

try:
    import pandas as pd
except ImportError:
    pd = None

from gretel_client.transformers import (
    RedactWithCharConfig,
    DataPath,
    DataTransformPipeline,
    StringMask,
)


xf = [RedactWithCharConfig()]
xf2 = [RedactWithCharConfig(char="Y")]

paths = [
    DataPath(input="foo", xforms=xf),
    DataPath(input="bar", xforms=xf2),
    DataPath(input="*"),
]

pipe = DataTransformPipeline(paths)

rec = {"foo": "hello", "bar": "there", "baz": "world"}

out = pipe.transform_record(rec)

assert out == {"foo": "XXXXX", "bar": "YYYYY", "baz": "world"}

print(out)

# Now let's do partial redactions

mask_1 = StringMask(
    start_pos=3
)  # let's only keep the first few chars of an email address
mask_2 = StringMask(
    mask_after="@"
)  # let's only mask the domain part of the email address
xf_1 = [RedactWithCharConfig(mask=[mask_1])]
xf_2 = [RedactWithCharConfig(mask=[mask_2])]

paths = [
    DataPath(input="email", xforms=[xf_1]),
    DataPath(input="email_2", xforms=[xf_2]),
    DataPath(input="*"),
]

pipe = DataTransformPipeline(paths)

rec = {
    "email": "mongtomery.burns@springfield.net",
    "email_2": "homer.j.simpson@springfield.net",
}

out = pipe.transform_record(rec)

print(out)

assert out == {
    "email": "monXXXXXXX.XXXXX@XXXXXXXXXXX.XXX",
    "email_2": "homer.j.simpson@XXXXXXXXXXX.XXX",
}

####################
# DataFrame Version
####################

if pd is None:
    print("Skipping DataFrame version, Pandas not installed!")
    sys.exit(1)

records = [
    {"name": "Homer", "id": 1234, "email": "homer.j.simpson@springfield.net"},
    {"name": "Monty", "id": 5678, "email_2": "mongtomery.burns@springfield.net"},
]

df = pd.DataFrame(records)

transformed_df = pipe.transform_df(df)

assert transformed_df.to_dict(orient="records") == [
    {
        "email": "homXX.X.XXXXXXX@XXXXXXXXXXX.XXX",
        "email_2": None,
        "id": 1234,
        "name": "Homer",
    },
    {
        "email": None,
        "email_2": "mongtomery.burns@XXXXXXXXXXX.XXX",
        "id": 5678,
        "name": "Monty",
    },
]
