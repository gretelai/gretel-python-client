# GlobalsOutput


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error_rate** | **float** |  | [optional] [default to 0.2]
**model_configs** | [**List[ModelConfigOutput]**](ModelConfigOutput.md) |  | [optional] 
**model_suite** | **str** |  | [optional] [default to 'apache-2.0']
**num_records** | **int** |  | [optional] [default to 100]

## Example

```python
from gretel_client._api.models.globals_output import GlobalsOutput

# TODO update the JSON string below
json = "{}"
# create an instance of GlobalsOutput from a JSON string
globals_output_instance = GlobalsOutput.from_json(json)
# print the JSON string representation of the object
print(GlobalsOutput.to_json())

# convert the object into a dict
globals_output_dict = globals_output_instance.to_dict()
# create an instance of GlobalsOutput from a dict
globals_output_from_dict = GlobalsOutput.from_dict(globals_output_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


