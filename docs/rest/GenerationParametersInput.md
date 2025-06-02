# GenerationParametersInput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**max_tokens** | **int** |  | [optional] 
**temperature** | [**Temperature**](Temperature.md) |  | [optional] 
**top_p** | [**TopP**](TopP.md) |  | [optional] 

## Example

```python
from gretel_client._api.models.generation_parameters_input import GenerationParametersInput

# TODO update the JSON string below
json = "{}"
# create an instance of GenerationParametersInput from a JSON string
generation_parameters_input_instance = GenerationParametersInput.from_json(json)
# print the JSON string representation of the object
print(GenerationParametersInput.to_json())

# convert the object into a dict
generation_parameters_input_dict = generation_parameters_input_instance.to_dict()
# create an instance of GenerationParametersInput from a dict
generation_parameters_input_from_dict = GenerationParametersInput.from_dict(generation_parameters_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


