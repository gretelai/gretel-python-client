# ModelConfigOutput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**alias** | **str** |  | 
**connection_id** | **str** |  | [optional] 
**generation_parameters** | [**GenerationParametersOutput**](GenerationParametersOutput.md) |  | 
**is_reasoner** | **bool** |  | [optional] [default to False]
**model_name** | **str** |  | 

## Example

```python
from gretel_client._api.models.model_config_output import ModelConfigOutput

# TODO update the JSON string below
json = "{}"
# create an instance of ModelConfigOutput from a JSON string
model_config_output_instance = ModelConfigOutput.from_json(json)
# print the JSON string representation of the object
print(ModelConfigOutput.to_json())

# convert the object into a dict
model_config_output_dict = model_config_output_instance.to_dict()
# create an instance of ModelConfigOutput from a dict
model_config_output_from_dict = ModelConfigOutput.from_dict(model_config_output_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


