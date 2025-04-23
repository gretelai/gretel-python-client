# LLMInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**generation_params** | **object** |  | 
**model_name** | **str** |  | 

## Example

```python
from gretel_client._api.models.llm_info import LLMInfo

# TODO update the JSON string below
json = "{}"
# create an instance of LLMInfo from a JSON string
llm_info_instance = LLMInfo.from_json(json)
# print the JSON string representation of the object
print(LLMInfo.to_json())

# convert the object into a dict
llm_info_dict = llm_info_instance.to_dict()
# create an instance of LLMInfo from a dict
llm_info_from_dict = LLMInfo.from_dict(llm_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


