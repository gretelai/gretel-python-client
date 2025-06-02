# GenerationParametersOutput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**max_tokens** | **int** |  | [optional] 
**temperature** | [**Temperature**](Temperature.md) |  | [optional] 
**top_p** | [**TopP**](TopP.md) |  | [optional] 

## Example

```python
from gretel_client._api.models.generation_parameters_output import GenerationParametersOutput

# TODO update the JSON string below
json = "{}"
# create an instance of GenerationParametersOutput from a JSON string
generation_parameters_output_instance = GenerationParametersOutput.from_json(json)
# print the JSON string representation of the object
print(GenerationParametersOutput.to_json())

# convert the object into a dict
generation_parameters_output_dict = generation_parameters_output_instance.to_dict()
# create an instance of GenerationParametersOutput from a dict
generation_parameters_output_from_dict = GenerationParametersOutput.from_dict(generation_parameters_output_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


