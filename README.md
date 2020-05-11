# Gretel Python Client

The Gretel Python Client allows you to interact with the Gretel REST API.  Where possible, we have added specific conveinence to help auto-manage things like ingest of large amounts of records. Please see the getting started section and the module documentation for more details.

In order to use this client, you must have a valid API key. Please login to [our console](https://aura.now.sh) in order to get one.

# Getting Started

The easiest way to get started is by creating a `Project` instance that allows you to directly interface with a Gretel project.

Gretel will automatically manage API calls based on the project instance you are working with.

First, you will need to create a `Client` instance:

```python
from gretel_client import get_cloud_client

client = get_cloud_client('api', 'your_api_key')
```

Next, you can create a `Project` instance. This high-level object let's you interact direclty
with your project. To do this, you can use the `get_project` factory method that is part of the `Client` instance.

## Projects

### Get an existing project

You can get an existing project by:

```python
project = client.get_project(name='my_existing_project')
```

This will raise a `BadRequest` error if the project does not exist or you are not a member.

### Create a project

You can create a new named project by using the `create` flag.

```python
project = client.get_project(name='my_named_project', create=True)
```

If the project exists _or_ the name is available, a `Project` instance is returned. Otherwise
a `BadRequest` is raised.

Optionally, if a name does not matter, you can use an auto-named project and Gretel will
create one for you:

```python
project = client.get_project(create=True)
```

You can see the name Gretel created by checking `project.name`.

## Sending Records