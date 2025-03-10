# TaskValidationResult

Used to validate a task is configured properly. When a task is validated, the entire Task is instantiated and deeper validation business logic may be performed. This is notably different from schema validation that simply ensures the task config schema is well-formed.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** |  | [optional] [default to '']
**valid** | **bool** |  | 

## Example

```python
from gretel_client._api.models.task_validation_result import TaskValidationResult

# TODO update the JSON string below
json = "{}"
# create an instance of TaskValidationResult from a JSON string
task_validation_result_instance = TaskValidationResult.from_json(json)
# print the JSON string representation of the object
print(TaskValidationResult.to_json())

# convert the object into a dict
task_validation_result_dict = task_validation_result_instance.to_dict()
# create an instance of TaskValidationResult from a dict
task_validation_result_from_dict = TaskValidationResult.from_dict(task_validation_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


