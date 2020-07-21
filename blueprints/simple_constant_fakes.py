"""
This blueprint shows how you can take data from a field, a feed it into
a transformer that will create a constant type of fake entity using the
original value.

A seed value is required for the transformers. Using the same seed will
help ensure that you get the same fake value for any given input value
"""
from gretel_client.transformers import DataPath, DataTransformPipeline
from gretel_client.transformers import FakeConstantConfig

SEED = 8675309

SOURCE = [
    {
        "activity": "Wedding Crasher",
        "guest": "Seamus O'Toole",
        "location": "Washington DC",
    },
    {
        "activity": "Wedding Crasher",
        "guest": "Bobby O'Shea",
        "location": "Baltimore"
    },
]

guest_xf = FakeConstantConfig(seed=SEED, fake_method="name")
location_xf = FakeConstantConfig(seed=SEED, fake_method="city")

paths = [
    DataPath(input="guest", xforms=[guest_xf]),
    DataPath(input="location", xforms=[location_xf]),
    DataPath(input="*"),
]

pipe = DataTransformPipeline(paths)

results = []

for record in SOURCE:
    results.append(pipe.transform_record(record))

assert results == [
    {
        "activity": "Wedding Crasher",
        "guest": "Sean Johnson",
        "location": "Smithtown"
    },
    {
        "activity": "Wedding Crasher",
        "guest": "Christopher Obrien",
        "location": "Katiebury",
    },
]

print(results)
