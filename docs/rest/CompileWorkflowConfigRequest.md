# CompileWorkflowConfigRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | **object** |  | 
**config_type** | [**SourceConfigType**](SourceConfigType.md) |  | 

## Example

```python
from gretel_client._api.models.compile_workflow_config_request import CompileWorkflowConfigRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CompileWorkflowConfigRequest from a JSON string
compile_workflow_config_request_instance = CompileWorkflowConfigRequest.from_json(json)
# print the JSON string representation of the object
print(CompileWorkflowConfigRequest.to_json())

# convert the object into a dict
compile_workflow_config_request_dict = compile_workflow_config_request_instance.to_dict()
# create an instance of CompileWorkflowConfigRequest from a dict
compile_workflow_config_request_from_dict = CompileWorkflowConfigRequest.from_dict(compile_workflow_config_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


