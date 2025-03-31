# CompileWorkflowConfigResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**workflow** | **object** |  | 

## Example

```python
from gretel_client._api.models.compile_workflow_config_response import CompileWorkflowConfigResponse

# TODO update the JSON string below
json = "{}"
# create an instance of CompileWorkflowConfigResponse from a JSON string
compile_workflow_config_response_instance = CompileWorkflowConfigResponse.from_json(json)
# print the JSON string representation of the object
print(CompileWorkflowConfigResponse.to_json())

# convert the object into a dict
compile_workflow_config_response_dict = compile_workflow_config_response_instance.to_dict()
# create an instance of CompileWorkflowConfigResponse from a dict
compile_workflow_config_response_from_dict = CompileWorkflowConfigResponse.from_dict(compile_workflow_config_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


