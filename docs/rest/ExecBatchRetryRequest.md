# ExecBatchRetryRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**workflow** | **object** |  | [optional] 
**workflow_run_id** | **str** |  | 
**workflow_run_name** | **str** |  | [optional] 

## Example

```python
from gretel_client._api.models.exec_batch_retry_request import ExecBatchRetryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ExecBatchRetryRequest from a JSON string
exec_batch_retry_request_instance = ExecBatchRetryRequest.from_json(json)
# print the JSON string representation of the object
print(ExecBatchRetryRequest.to_json())

# convert the object into a dict
exec_batch_retry_request_dict = exec_batch_retry_request_instance.to_dict()
# create an instance of ExecBatchRetryRequest from a dict
exec_batch_retry_request_from_dict = ExecBatchRetryRequest.from_dict(exec_batch_retry_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


