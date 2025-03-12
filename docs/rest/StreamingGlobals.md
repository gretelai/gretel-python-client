# StreamingGlobals


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error_rate** | **float** |  | [optional] [default to 1.0]
**model_configs** | [**List[ModelConfigInput]**](ModelConfigInput.md) |  | [optional] 
**model_suite** | **str** |  | [optional] [default to 'apache-2.0']
**num_records** | **int** |  | [optional] [default to 50]

## Example

```python
from gretel_client._api.models.streaming_globals import StreamingGlobals

# TODO update the JSON string below
json = "{}"
# create an instance of StreamingGlobals from a JSON string
streaming_globals_instance = StreamingGlobals.from_json(json)
# print the JSON string representation of the object
print(StreamingGlobals.to_json())

# convert the object into a dict
streaming_globals_dict = streaming_globals_instance.to_dict()
# create an instance of StreamingGlobals from a dict
streaming_globals_from_dict = StreamingGlobals.from_dict(streaming_globals_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


