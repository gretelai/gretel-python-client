# GlobalsInput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error_rate** | **float** |  | [optional] 
**model_configs** | [**List[ModelConfigInput]**](ModelConfigInput.md) |  | [optional] 
**model_suite** | **str** |  | [optional] 
**num_records** | **int** |  | [optional] 

## Example

```python
from gretel_client._api.models.globals_input import GlobalsInput

# TODO update the JSON string below
json = "{}"
# create an instance of GlobalsInput from a JSON string
globals_input_instance = GlobalsInput.from_json(json)
# print the JSON string representation of the object
print(GlobalsInput.to_json())

# convert the object into a dict
globals_input_dict = globals_input_instance.to_dict()
# create an instance of GlobalsInput from a dict
globals_input_from_dict = GlobalsInput.from_dict(globals_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


