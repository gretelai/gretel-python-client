# ManualDistributionParams


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**values** | **List[float]** |  | 
**weights** | **List[float]** |  | [optional] 

## Example

```python
from gretel_client._api.models.manual_distribution_params import ManualDistributionParams

# TODO update the JSON string below
json = "{}"
# create an instance of ManualDistributionParams from a JSON string
manual_distribution_params_instance = ManualDistributionParams.from_json(json)
# print the JSON string representation of the object
print(ManualDistributionParams.to_json())

# convert the object into a dict
manual_distribution_params_dict = manual_distribution_params_instance.to_dict()
# create an instance of ManualDistributionParams from a dict
manual_distribution_params_from_dict = ManualDistributionParams.from_dict(manual_distribution_params_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


