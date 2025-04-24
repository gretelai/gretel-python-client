Safe Synthetics SDK
-------------------
Gretel Safe Synthetics allows you to create private versions of your sensitive data. You can use Safe Synthetics to redact and replace sensitive Personally Identifiable Information (PII) with Transform, obfuscate quasi-identifiers with Synthetics, and apply differential privacy for mathematical guarantees of privacy protection.

Getting Started
===============

The Safe Synthetics SDK adopts a fluent builder pattern for constructing a Safe Synthetic data pipeline.

The following code example creates and executes a synthetic data pipeline. The script will wait while the job completes, and then return the data generation report and associated dataset.

.. code-block:: python

    from gretel_client.navigator_client import Gretel

    gretel = Gretel()

    synthetic_dataset = gretel.safe_synthetics\
        .from_data_source("https://raw.githubusercontent.com/gretelai/gretel-blueprints/refs/heads/main/sample_data/financial_transactions.csv")
        .transform()
        .synthesize()
        .create()


    # waits until the synthetic data pipeline completes
    synthetic_dataset.wait_until_done()

    # to view a report of the dataset
    synthetic_dataset.report.table


    # to view the final transformed dataset as a dataframe
    synthetic_dataset.dataset.df

For more information and examples please view our main `product documentation <https://docs.gretel.ai/create-synthetic-data/safe-synthetics/sdk>`_.


.. autoclass:: gretel_client.safe_synthetics.dataset.SafeSyntheticDataset
    :members:
    :member-order: bysource
