# ManualDistribution


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**distribution_type** | [**DistributionType**](DistributionType.md) |  | [optional] 
**params** | [**ManualDistributionParams**](ManualDistributionParams.md) |  | 

## Example

```python
from gretel_client._api.models.manual_distribution import ManualDistribution

# TODO update the JSON string below
json = "{}"
# create an instance of ManualDistribution from a JSON string
manual_distribution_instance = ManualDistribution.from_json(json)
# print the JSON string representation of the object
print(ManualDistribution.to_json())

# convert the object into a dict
manual_distribution_dict = manual_distribution_instance.to_dict()
# create an instance of ManualDistribution from a dict
manual_distribution_from_dict = ManualDistribution.from_dict(manual_distribution_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


