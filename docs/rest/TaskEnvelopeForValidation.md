# TaskEnvelopeForValidation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | **object** |  | 
**globals** | **object** |  | [optional] 
**inputs** | [**List[TaskInput]**](TaskInput.md) |  | [optional] 
**name** | **str** |  | 

## Example

```python
from gretel_client._api.models.task_envelope_for_validation import TaskEnvelopeForValidation

# TODO update the JSON string below
json = "{}"
# create an instance of TaskEnvelopeForValidation from a JSON string
task_envelope_for_validation_instance = TaskEnvelopeForValidation.from_json(json)
# print the JSON string representation of the object
print(TaskEnvelopeForValidation.to_json())

# convert the object into a dict
task_envelope_for_validation_dict = task_envelope_for_validation_instance.to_dict()
# create an instance of TaskEnvelopeForValidation from a dict
task_envelope_for_validation_from_dict = TaskEnvelopeForValidation.from_dict(task_envelope_for_validation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


