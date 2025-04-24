Data Designer SDK
-----------------

|:wave:| Hi! Welcome to the `DataDesigner` SDK documentation.

`DataDesigner` is a general-purpose framework for designing and generating synthetic datasets from scratch. It provides a simple interface that lets developers describe the attributes of the dataset they want and iterate on the generated data through fast previews and detailed evaluations.


Getting Started
===============

The best way to learn how to use `DataDesigner` is by example. 

To get started, here is a simple example of how to generate a product review dataset.

.. code-block:: python

    from gretel_client.navigator_client import Gretel

    # We import AIDD column and parameter types using this shorthand for convenience.
    import gretel_client.data_designer.params as P
    import gretel_client.data_designer.columns as C
    
    gretel = Gretel()

    # Initialize a new Data Designer instance using the `data_designer` factory.
    # We use the "apache-2.0" model suite to generate data with a permissive license.
    aidd = gretel.data_designer.new(model_suite="apache-2.0")

    ##############################################################
    # Add Sampler columns to our data design.
    ##############################################################

    aidd.add_column(
        C.SamplerColumn(
            name="product_category", 
            type=P.SamplerType.CATEGORY,
            params=P.CategorySamplerParams(
                values=["Electronics", "Clothing", "Home & Kitchen", "Books", "Home Office"], 
            )
        )
    )

    # This column will sample synthetic person data based on the US Census.
    aidd.add_column(
        C.SamplerColumn(
            name="customer",
            type=P.SamplerType.PERSON,
            params=P.PersonSamplerParams(age_range=[18, 70])
        )
    )

    aidd.add_column(
        C.SamplerColumn(
            name="number_of_stars",
            type=P.SamplerType.UNIFORM,
            params=P.UniformSamplerParams(low=1, high=5),
            convert_to="int"
        )
    )

    ##############################################################
    # Add LLM-generated columns to our data design.
    ##############################################################

    aidd.add_column(
        C.LLMTextColumn(
            name="product_name",
            # All columns in the dataset are accessible in the prompt template.
            prompt=(
                "Come up with a creative product name for a product in the '{{ product_category }}' category. "
                "Respond with only the product name, no other text."
            ),
            # This is optional but can be useful for controlling the LLM's behavior. Do not include instructions
            # related to output formatting in the system prompt, as AIDD handles this based on the column type.
            system_prompt=(
                "You are a helpful assistant that generates product names. You respond with only the product name, "
                "no other text. You do NOT add quotes around the product name. "
            )
        )
    )

    aidd.add_column(
        C.LLMTextColumn(
            name="customer_review",
            # Note the nested JSON of the customer column is accessible using dot notation.
            prompt=(
                "You are a customer named {{ customer.first_name }} from {{ customer.city }}, {{ customer.state }}. "
                "You are {{ customer.age }} years old and recently purchased a product called {{ product_name }}. "
                "Write a review of this product, which you gave a rating of {{ number_of_stars }} stars. "
            ),
        )
    )

    ##############################################################
    # Generate your dataset!
    ##############################################################

    # Generate 10 preview records in real time for fast iteration.
    preview = aidd.preview()

    # Create a new dataset with an evaluation report.
    workflow_run = aidd.with_evaluation_report().create(
        num_records=100, 
        name="aidd-sdk-101-product-reviews", 
        wait_until_done=True
    )

    # Download the dataset to a pandas DataFrame.
    df = workflow_run.dataset.df


|:dividers:| API Reference
==========================

The `DataDesigner` class serves as a high-level interface for building datasets from scratch.

.. autoclass:: gretel_client.data_designer.data_designer.DataDesigner()
   :members:
   :member-order: bysource


|:classical_building:| Column Types
===================================

To craft your data design, you can select from the following column types.

.. automodule:: gretel_client.data_designer.columns
   :members:
   :member-order: bysource


|:game_die:| Samplers
=====================

Samplers are an important concept in `DataDesigner` for non-LLM based synthetic data generation.

.. autoclass:: gretel_client.workflows.configs.tasks.SamplerType() 
   :members:
   :undoc-members:

.. automodule:: gretel_client.data_designer.params
   :members:
   :member-order: alphabetical


|:hand:| Constraints
====================

Constraints are used to ensure that data generated by samplers follows any relevant business rules or constraints.

.. autoclass:: gretel_client.workflows.configs.tasks.ConstraintType() 
   :members:
   :undoc-members:

.. autoclass:: gretel_client.workflows.configs.tasks.InequalityOperator() 
   :members:
   :undoc-members:

.. autoclass:: gretel_client.workflows.configs.tasks.ColumnConstraint
   :members:
   :member-order: bysource

.. autoclass:: gretel_client.workflows.configs.tasks.ColumnConstraintParams
   :members:
    :member-order: bysource
