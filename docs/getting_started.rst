.. toctree::
   :maxdepth: 4


Getting Started
---------------

Looking to get started creating synthetic data from an existing dataset? Use the :doc:`safe_synthetics`.

Looking to create data from scratch? Use the :doc:`data_designer`.

For more examples and notebooks demonstrating Gretel's product, please see our blueprint repository at `github.com/gretelai/gretel-blueprints <https://github.com/gretelai/gretel-blueprints>`_.

You can find our main product documentation at `docs.gretel.ai <https://docs.gretel.ai/>`_.

Configuring the Client
======================

To get started with Gretel, you first construct a ``Gretel`` client for your session. That client provides access to all of Gretel's APIs via a python SDK interface.


To instantiate the client

.. code-block:: python

    from gretel_client.navigator_client import Gretel

    gretel = Gretel(api_key="prompt")

The example above will instantiate a client and prompt for an API key if one isn't already configured on the system.

An API key can alternatively be provided directly to the constructor via the `api_key` param, or configured on the ``GRETEL_API_KEY`` environment variable.

Projects
========

Projects are an organizational construct to help manage resources, control access permissions, and enable collaboration through sharing features.

Each client session is configured with a default project configured on client instantiation

.. code-block:: python

    gretel = Gretel(default_project_id="my-project")

If the project exists that project will get loaded. If the project does not exist, the project will be created automatically and reused for the session.

If no project is specified a default project for the SDK will automatically get created.

You can also create a temporary project using ``gretel.temp_project()``. This method is implemented as a context manager. Once you leave the scope of the block, the project is deleted automatically

.. code-block:: python

    with gretel.tmp_project() as tmp_gretel_client:
        ...

For more product details related to Projects, please see our docs `here <https://docs.gretel.ai/gretel-basics/getting-started/projects>`_.

Workflows
=========

Gretel Workflows provide an easy to use, config driven API for building synthetic data pipelines.

The :doc:`safe_synthetics` and :doc:`data_designer` construct Workflows automatically for you using declarative APIs, but may also construct your own Workflow.

If you're just getting start with Gretel, we recommend you start with those high-level APIs before attempting to construct your own Workflows.

There are two options for constructing a Workflow from scratch. Using a fluent builder interface or passing a list of tasks to a ``workflows.create(...)`` method

Using the fluent builder

.. code-block:: python

    my_workflow = gretel.workflows.builder() \
        .add_step(gretel.tasks.DataSource(data_source="...")) \
        .add_step(gretel.tasks.Transform()) \
        .run()


Or constructing a list of tasks

.. code-block:: python

    my_workflow = gretel.workflows.create([
        gretel.tasks.DataSource(data_source="...")
        gretel.tasks.Transform()
    ])

You can find an exhaustive list of tasks under :class:`gretel_client.workflows.configs.registry.Registry`.

Once the workflow has been created, a :class:`gretel_client.workflows.workflow.WorkflowRun` object is returned. This class represents a concrete Workflow Run.

To block the current thread and stream logs for the current run you can call

.. code-block:: python

    my_workflow.wait_until_done()

Workflow Runs can be viewed in the Gretel console by calling

.. code-block:: python

    my_workflow.console_url()

Once a Workflow completes, you can access :class:`datasets<gretel_client.workflows.io.Dataset>`, :class:`reports<gretel_client.workflows.io.Report>`, and individual step outputs.


.. code-block:: python

    # Access the final Dataset produce by the Workflow as a Dataframe
    my_workflow.dataset.df

    # View the report for a Workflow
    my_workflow.report.table

    # Access individual step outputs
    my_workflow.get_step_out("transform")

You can load existing :class:`WorkflowRun<gretel_client.workflows.workflow.WorkflowRun>` runs with

.. code-block:: python

    my_other_workflow = gretel.workflows.get_workflow_run("workflow run id here")

**Module Reference**

.. toctree::
   :maxdepth: 2

   workflows


Files
=====

The Files API provides a mechanism to upload data to Gretel and use it as inputs to a Workflow. You can upload files as remote URLs, local file paths, or in memory Dataframes.


.. code-block:: python

    from gretel_client.navigator_client import Gretel

    gretel = Gretel()

    file = gretel.files.upload("your_file.csv")

    my_workflow = gretel.workflows.create([
        gretel.tasks.DataSource(data_source=file.id)
        gretel.tasks.Transform()
    ])



**Module Reference**

.. toctree::
   :maxdepth: 2

   files


