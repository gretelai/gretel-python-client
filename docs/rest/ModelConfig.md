# ModelConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**alias** | **str** |  | 
**generation_parameters** | [**GenerationParameters**](GenerationParameters.md) |  | 
**model_name** | **str** |  | 

## Example

```python
from gretel_client._api.models.model_config import ModelConfig

# TODO update the JSON string below
json = "{}"
# create an instance of ModelConfig from a JSON string
model_config_instance = ModelConfig.from_json(json)
# print the JSON string representation of the object
print(ModelConfig.to_json())

# convert the object into a dict
model_config_dict = model_config_instance.to_dict()
# create an instance of ModelConfig from a dict
model_config_from_dict = ModelConfig.from_dict(model_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


