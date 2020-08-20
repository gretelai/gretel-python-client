# Introduction: Gretel Transformers

Welcome to the Gretel Transformers documentation!  Here we will introduce you to the concepts in the Transformers sub-package and
provide some basic tutorials for getting started.

For more advanced usage, please refer to our [blueprints](https://github.com/gretelai/gretel-python-client/tree/master/blueprints).

## Installation

Most transformers are installed with installing the client. For transformers that utilize Format Preserving Encryption,
you will need to intstall the extras for that:

```
$ pip install gretel-client[fpe]
```

## Basics

Our Transformers SDK allows the transformation of data records, generally in the form of a Python dictionary, at two different levels:

1) **Field level**: You can easily apply different transforms to any key within a Python dictionary. In this SDK we refer to a `record` interchangeably as a `dict`. And a key within a `record` / `dict` can be considered a field.  A field level transformation occurs on either the entire field or a portion of the field, depending on the transformation technique.  The transformation is applied based on matching field names with transformation configurations. You may apply field level transformations directly to records without consuming data from the Gretel API.

2) **Entity level**: The SDK can also consume records from the Gretel API that contain record metadata and the results of our entity detection and labeling. Instead of needing to define the specific name of a field, you may utilize a label string to apply specific transforms to. For example, you may apply a character redaction to _any_ field that contains an `email_address` entity without ever having to know exactly which field that data resides in.

## Components

The fundamental components of creating a transformation workflow are:

- One or more transformation configurations
- One or more data pathways
- A single tranfsormation pipeline which applies configurations to a record

Let's explore these components with some code snippets.  In these examples, we will use a very simple
transformer known as the character redactor. This simply replaces any matching data with a sequence of
a single redaction character, which defaults to `X`.

### Configurations

Transformer configurations are simply data containers that get exchanged for specific Transformer instances
under the hood.  Each configuration takes a variety of parameters based on the transformer's functionality.

One common parameter that configurations can use is the `labels` parameter which can take a list of Gretel entity labels.

Typically, when you create a transform configuration, you can place them into a list, since transformers work serially.

Create a field-level character redactor:

```python
from gretel_client.transformers.transformers import RedactWithCharConfig

xf = [RedactWithCharConfig(char="Y")]  # NOTE: we default to "X" if not provided
```

Create an entity-level character redactor:

```python
from gretel_client.transformers.transformers import RedactWithCharConfig

xf = [RedactWithCharConfig(char="Y", labels=["email_address"])]
```

### Data Paths

A `DataPath` is the application of a sequence of transformations to one or more fields from the source record.  If more than one transformer is provided to a path, they will be executed in order.

**NOTE**: Only _one_ data path will ever be run against any single field. The first `DataPath` that matches a field is the one that will be used, all others will get discarded.

Data paths have three core params:

- `input`: This matches the path to one or more fields. Exact field names or Glob-style patterns can be used here.
    - `customer_id` would match a field named "customer_id" exactly
    - `customer_*` would match any field that has "customer_" as a prefix
    - `*` matches every field
- `xforms`: This is a list of transformation configurations. These will be run in order on all fields that match.
- `output`: Optionally, you may change the name of the field that the transformed value is loaded into.

Data paths should be defined in a list. They are matched against the source record fields in order.

**Example: Field Level**

Let's redact all `name`, `address`, and `location` fields.

```python
from gretel_client.transformers.transformers import RedactWithCharConfig
from gretel_client.transformers import DataPath

xf = [RedactWithCharConfig(char="Y")]  # NOTE: we default to "X" if not provided

paths = [
    DataPath(input="name", xforms=xf),
    DataPath(input="address", xforms=xf),
    DataPath(input="location", xforms=xf),
    DataPath(input="*")  # NOTE: This keeps all other source fields and values in tact
]
```

It's important to note that the final `DataPath` keeps all other fields in the record as-is. If you removed it, only
the three defined fields would be in the transformed record.

**Example: Entity Level**

Let's redact all `email_address` occurances, regardless of field

```python
from gretel_client.transformers.transformers import RedactWithCharConfig
from gretel_client.transformers import DataPath

xf = [RedactWithCharConfig(char="Y"), labels=["email_address"])] 

paths = [
    DataPath(input="*", xforms=xf)  # Redact all email_addresses everywhere!
]
```

**Example: Entity + Field Level**

Let's redact all `email_address` occurances only in the `desc` and `detail` fields.

```python
from gretel_client.transformers.transformers import RedactWithCharConfig
from gretel_client.transformers import DataPath

xf = [RedactWithCharConfig(char="Y"), labels=["email_address"])] 

paths = [
    DataPath(input="desc", xforms=xf),
    DataPath(input="detail", xforms=xf),
    DataPath(input="*")
]
```

### Data Transformation Pipeline

Now that data paths are created, the final step is loading the paths into a single `DataTransformPipeline`.  This object
handles the routing of source records through the data paths and returning a transformed record. The entire pipeline
can be executed via the `transform_record()` method.

**Example**

```python
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


"""
out:

{
    "foo": "XXXXX",
    "bar": "YYYYY",
    "baz": "world
}
"""
```
