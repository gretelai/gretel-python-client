# LLMSuiteConfigWithGenerationParams


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**license** | **str** |  | 
**model_aliases** | **Dict[str, str]** |  | 
**models** | [**List[LLMInfo]**](LLMInfo.md) |  | 
**suite_name** | **str** |  | 

## Example

```python
from gretel_client._api.models.llm_suite_config_with_generation_params import LLMSuiteConfigWithGenerationParams

# TODO update the JSON string below
json = "{}"
# create an instance of LLMSuiteConfigWithGenerationParams from a JSON string
llm_suite_config_with_generation_params_instance = LLMSuiteConfigWithGenerationParams.from_json(json)
# print the JSON string representation of the object
print(LLMSuiteConfigWithGenerationParams.to_json())

# convert the object into a dict
llm_suite_config_with_generation_params_dict = llm_suite_config_with_generation_params_instance.to_dict()
# create an instance of LLMSuiteConfigWithGenerationParams from a dict
llm_suite_config_with_generation_params_from_dict = LLMSuiteConfigWithGenerationParams.from_dict(llm_suite_config_with_generation_params_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


