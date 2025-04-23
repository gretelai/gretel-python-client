# ValidateModelSuiteRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**model_configs** | **List[object]** |  | 

## Example

```python
from gretel_client._api.models.validate_model_suite_request import ValidateModelSuiteRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ValidateModelSuiteRequest from a JSON string
validate_model_suite_request_instance = ValidateModelSuiteRequest.from_json(json)
# print the JSON string representation of the object
print(ValidateModelSuiteRequest.to_json())

# convert the object into a dict
validate_model_suite_request_dict = validate_model_suite_request_instance.to_dict()
# create an instance of ValidateModelSuiteRequest from a dict
validate_model_suite_request_from_dict = ValidateModelSuiteRequest.from_dict(validate_model_suite_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


