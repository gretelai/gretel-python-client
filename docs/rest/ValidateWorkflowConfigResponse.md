# ValidateWorkflowConfigResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** |  | [optional] [default to '']
**valid** | **bool** |  | 

## Example

```python
from gretel_client._api.models.validate_workflow_config_response import ValidateWorkflowConfigResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ValidateWorkflowConfigResponse from a JSON string
validate_workflow_config_response_instance = ValidateWorkflowConfigResponse.from_json(json)
# print the JSON string representation of the object
print(ValidateWorkflowConfigResponse.to_json())

# convert the object into a dict
validate_workflow_config_response_dict = validate_workflow_config_response_instance.to_dict()
# create an instance of ValidateWorkflowConfigResponse from a dict
validate_workflow_config_response_from_dict = ValidateWorkflowConfigResponse.from_dict(validate_workflow_config_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


