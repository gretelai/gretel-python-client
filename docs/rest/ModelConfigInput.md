# ModelConfigInput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**alias** | **str** |  | 
**api_base** | **str** |  | [optional] 
**api_key** | **str** |  | [optional] 
**generation_parameters** | [**GenerationParametersInput**](GenerationParametersInput.md) |  | 
**model_name** | **str** |  | 

## Example

```python
from gretel_client._api.models.model_config_input import ModelConfigInput

# TODO update the JSON string below
json = "{}"
# create an instance of ModelConfigInput from a JSON string
model_config_input_instance = ModelConfigInput.from_json(json)
# print the JSON string representation of the object
print(ModelConfigInput.to_json())

# convert the object into a dict
model_config_input_dict = model_config_input_instance.to_dict()
# create an instance of ModelConfigInput from a dict
model_config_input_from_dict = ModelConfigInput.from_dict(model_config_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


