# ValidateModelSuiteResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**violations** | **List[str]** |  | 

## Example

```python
from gretel_client._api.models.validate_model_suite_response import ValidateModelSuiteResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ValidateModelSuiteResponse from a JSON string
validate_model_suite_response_instance = ValidateModelSuiteResponse.from_json(json)
# print the JSON string representation of the object
print(ValidateModelSuiteResponse.to_json())

# convert the object into a dict
validate_model_suite_response_dict = validate_model_suite_response_instance.to_dict()
# create an instance of ValidateModelSuiteResponse from a dict
validate_model_suite_response_from_dict = ValidateModelSuiteResponse.from_dict(validate_model_suite_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


