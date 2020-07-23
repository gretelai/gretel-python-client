"""
Demonstration on how to use String-based Format Preserving Encryption
"""

from gretel_client.transformers import (
    FpeStringConfig,
    DataPath,
    DataTransformPipeline,
    DataRestorePipeline,
)

rec = {
    "Address": "317 Massa. Av.",
    "City": "Didim",
    "Country": "Eritrea",
    "Credit Card": "601128 2195205 818",
    "Customer ID": "169/61*009 38-34",
    "Date": "2019-10-08",
    "Name": "Grimes, Bo H.",
    "Zipcode": "745558",
}
field_xf = FpeStringConfig(
    secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10
)
data_paths = [
    DataPath(input="Credit Card", xforms=field_xf),
    DataPath(input="Customer ID", xforms=field_xf),
    DataPath(input="*"),
]
xf = DataTransformPipeline(data_paths)
rf = DataRestorePipeline(data_paths)
transformed = xf.transform_record(rec)
assert transformed["Credit Card"] == "447158 5942734 458"
assert transformed["Customer ID"] == "747/52*232 83-19"
restored = rf.transform_record(transformed)
assert restored == rec
