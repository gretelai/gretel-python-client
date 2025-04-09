# ExecBatchRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**project_id** | **str** |  | [optional] 
**workflow_config** | **object** |  | 
**workflow_id** | **str** |  | [optional] 
**workflow_run_name** | **str** |  | [optional] 

## Example

```python
from gretel_client._api.models.exec_batch_request import ExecBatchRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ExecBatchRequest from a JSON string
exec_batch_request_instance = ExecBatchRequest.from_json(json)
# print the JSON string representation of the object
print(ExecBatchRequest.to_json())

# convert the object into a dict
exec_batch_request_dict = exec_batch_request_instance.to_dict()
# create an instance of ExecBatchRequest from a dict
exec_batch_request_from_dict = ExecBatchRequest.from_dict(exec_batch_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


