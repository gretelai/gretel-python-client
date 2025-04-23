# GetModelSuitesResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**model_suites** | [**List[LLMSuiteConfigWithGenerationParams]**](LLMSuiteConfigWithGenerationParams.md) |  | 

## Example

```python
from gretel_client._api.models.get_model_suites_response import GetModelSuitesResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetModelSuitesResponse from a JSON string
get_model_suites_response_instance = GetModelSuitesResponse.from_json(json)
# print the JSON string representation of the object
print(GetModelSuitesResponse.to_json())

# convert the object into a dict
get_model_suites_response_dict = get_model_suites_response_instance.to_dict()
# create an instance of GetModelSuitesResponse from a dict
get_model_suites_response_from_dict = GetModelSuitesResponse.from_dict(get_model_suites_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


