# GenerationParameters


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**temperature** | **float** |  | 
**top_p** | **float** |  | 

## Example

```python
from gretel_client._api.models.generation_parameters import GenerationParameters

# TODO update the JSON string below
json = "{}"
# create an instance of GenerationParameters from a JSON string
generation_parameters_instance = GenerationParameters.from_json(json)
# print the JSON string representation of the object
print(GenerationParameters.to_json())

# convert the object into a dict
generation_parameters_dict = generation_parameters_instance.to_dict()
# create an instance of GenerationParameters from a dict
generation_parameters_from_dict = GenerationParameters.from_dict(generation_parameters_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


