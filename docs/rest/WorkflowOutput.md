# WorkflowOutput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**globals** | **object** |  | [optional] 
**inputs** | **object** |  | [optional] 
**name** | **str** |  | 
**steps** | [**List[Step]**](Step.md) |  | [optional] 
**version** | **str** |  | [optional] [default to '2']

## Example

```python
from gretel_client._api.models.workflow_output import WorkflowOutput

# TODO update the JSON string below
json = "{}"
# create an instance of WorkflowOutput from a JSON string
workflow_output_instance = WorkflowOutput.from_json(json)
# print the JSON string representation of the object
print(WorkflowOutput.to_json())

# convert the object into a dict
workflow_output_dict = workflow_output_instance.to_dict()
# create an instance of WorkflowOutput from a dict
workflow_output_from_dict = WorkflowOutput.from_dict(workflow_output_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


