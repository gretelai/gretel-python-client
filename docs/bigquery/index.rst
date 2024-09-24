Google BigQuery Integration
----------------------------

The Gretel Client provides interfaces that make it easy to
work directly with BigQuery DataFrames via the ``bigframes``
Python package.

In order to use the BigQuery integration, you must install
``bigframes`` directly. It is not installed as a dependency
of the Gretel SDK.

**NOTE**: In this module, DataFrame inputs and outputs are BigQuery DataFrame types.

The BigQuery module integrates with the following Gretel services:

- Gretel Synthetic Models such as Navigator Fine Tuning.
- Gretel Transforms which allows for concrete PII redaction and replacement, often as a pre-processing step to model fine-tuning.
- Gretel's Compound AI system: Navigator. Create, edit, and augment tabular data from a natural language prompt.

**Example Usage**

For example Python Notebooks, visit the `Gretel Blueprints Repository <https://github.com/gretelai/gretel-blueprints/tree/main/docs/notebooks/google>`_.

**Module Reference**

.. toctree::
     :maxdepth: 2
     
     bigframes
