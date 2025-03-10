# TaskEnvelope


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | **object** |  | 
**globals** | [**StreamingGlobals**](StreamingGlobals.md) |  | [optional] 
**inputs** | [**List[TaskInput]**](TaskInput.md) |  | [optional] 
**name** | **str** |  | 

## Example

```python
from gretel_client._api.models.task_envelope import TaskEnvelope

# TODO update the JSON string below
json = "{}"
# create an instance of TaskEnvelope from a JSON string
task_envelope_instance = TaskEnvelope.from_json(json)
# print the JSON string representation of the object
print(TaskEnvelope.to_json())

# convert the object into a dict
task_envelope_dict = task_envelope_instance.to_dict()
# create an instance of TaskEnvelope from a dict
task_envelope_from_dict = TaskEnvelope.from_dict(task_envelope_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


