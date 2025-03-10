# ExecBatchResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**project_id** | **str** |  | 
**workflow_id** | **str** |  | 
**workflow_run_id** | **str** |  | 

## Example

```python
from gretel_client._api.models.exec_batch_response import ExecBatchResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ExecBatchResponse from a JSON string
exec_batch_response_instance = ExecBatchResponse.from_json(json)
# print the JSON string representation of the object
print(ExecBatchResponse.to_json())

# convert the object into a dict
exec_batch_response_dict = exec_batch_response_instance.to_dict()
# create an instance of ExecBatchResponse from a dict
exec_batch_response_from_dict = ExecBatchResponse.from_dict(exec_batch_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


