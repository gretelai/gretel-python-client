Google Big Query Integration
----------------------------

The Gretel Client provides interfaces that make it easy to
work directly with BigQuery DataFrames via the ``bigframes``
Python package.

In order to use the Big Query integration, you must install
``bigframes`` directly. It is not installed as a dependency
of the Gretel SDK.

The BigQuery module integrates with the following Gretel services:

- BigQuery DataFrames can be used as input to Gretel Synthetic Models such as Navigator Fine Tuning.
- BigQuery DataFrames can be used as input to Gretel Transforms which allows for concrete PII redaction and replacement, often as a pre-processing step to model fine-tuning.
- BigQuery DataFrames can be used as input to Gretel's Compound AI system: Navigator. This allows for the creation of tabular data such as Synthetic Q/A pairs or editing of tables.

**Example Usage**

For example Python Notebooks, visit the `Gretel Blueprints Repository <https://github.com/gretelai/gretel-blueprints/tree/main/docs/notebooks/google>`_.

**Module Reference**

.. toctree::
     :maxdepth: 2
     
     bigframes
