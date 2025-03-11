# UniformDistribution


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**distribution_type** | [**DistributionType**](DistributionType.md) |  | [optional] 
**params** | [**UniformDistributionParams**](UniformDistributionParams.md) |  | 

## Example

```python
from gretel_client._api.models.uniform_distribution import UniformDistribution

# TODO update the JSON string below
json = "{}"
# create an instance of UniformDistribution from a JSON string
uniform_distribution_instance = UniformDistribution.from_json(json)
# print the JSON string representation of the object
print(UniformDistribution.to_json())

# convert the object into a dict
uniform_distribution_dict = uniform_distribution_instance.to_dict()
# create an instance of UniformDistribution from a dict
uniform_distribution_from_dict = UniformDistribution.from_dict(uniform_distribution_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


