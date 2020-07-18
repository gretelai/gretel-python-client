# Introduction: Client

The Gretel Python Client allows you to interact with the Gretel REST API.  Where possible, we have added specific conveinence to help auto-manage things like ingest of large amounts of records. Please see the getting started section and the module documentation for more details.

In order to use this client, you must have a valid API key. Please login to [our console](https://console.gretel.cloud) in order to get one.

## Installation

For basic installation:

```
$ pip install gretel-client
```

If you are working with Pandas and wish to enable the features for working with DataFrames:

```
$ pip install gretel-client[pandas]
```

## Getting Started

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

### Flushing and Deleting projects

Once you have a project instance you can flush and delete it.

Assuming an instance variable name of `project`, you can:

Flush:

This will purge all data from the metastore and delete cached annotated records. Your project namespace, permissions, and collaborators will still be there.

```python
project.flush()
```

Delete:

This will flush all data (see above) _and_ also delete the project shell (name, description), collaborators, and permissions.

```python
project.delete()
```

**NOTE:** Both of these commands run asyncronously in Gretel as they are called. So it may be a few moments for them to complete. Additionally, your `project` instance, if using `delete()` will not be usable anymore.

## Sending Records

The Gretel API consumes JSON-formatted records. There's a few ways that you can send these via the client.

### Dicts

You can send Python dictionaries directly using different methods.

First, you can send them on an API-call-by-API-call basis. When records are received by the Gretel API, each record is assigned a `gretel_id` and the status of each record (success or fail) is returned to the user. You can utilize this behavior with the `send()` method:

This method will return a tuple of `success` and `failure` notifications.

```
s, f = project.send({'foo': 'bar'})
```

The list of success items will look like:

```python
[{'idx': 0, 'gretel_id': '8afbd2c8f5b147c9bd30faffa3e5ad0a'}]
```

`idx` refers to the index in the original list of records that were sent. If you only sent a single record (a dict) then `idx` 0 will refer to that record.

The failure list will be empty if there were no issues on ingest.

You may also send a list of dicts. When using `send()` you will be subject to the max record size per API call, which is currently 50.

```python
data = [{'foo': 'bar'}, 1, 2, 3] 
s, f = project.send(data)
```

Here only the first record is valid:

```python
s
[{'idx': 0, 'gretel_id': '4c34d2fbc2974f81af2c07ac78eedcef'}]
```

And the last three were not valid JSON objects:


```python
f
[{'idx': 1,
  'message': 'Individual records must be JSON objects',
  'sender_fault': True},
 {'idx': 2,
  'message': 'Individual records must be JSON objects',
  'sender_fault': True},
 {'idx': 3,
  'message': 'Individual records must be JSON objects',
  'sender_fault': True}]
```

### Bulk Records

If you are streaming data or sending large amounts of records, you may not want to worry about using the `send()` method and chunking up your records. For this use case, you may use the `send_bulk()` method. It has the same signature as `send()` but the client will automatically chunk the input up and multithread to push the records to Gretel.

One exception to using this is that you will not receive any success or failure confirmations.

```python
data = [{f'foo_{i}': 'bar'} for i in range(500)]
project.send_bulk(data)

# >> 500 records [00:00, 157184.23records/s]
```
