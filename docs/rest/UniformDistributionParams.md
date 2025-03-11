# UniformDistributionParams


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**high** | **float** |  | 
**low** | **float** |  | 

## Example

```python
from gretel_client._api.models.uniform_distribution_params import UniformDistributionParams

# TODO update the JSON string below
json = "{}"
# create an instance of UniformDistributionParams from a JSON string
uniform_distribution_params_instance = UniformDistributionParams.from_json(json)
# print the JSON string representation of the object
print(UniformDistributionParams.to_json())

# convert the object into a dict
uniform_distribution_params_dict = uniform_distribution_params_instance.to_dict()
# create an instance of UniformDistributionParams from a dict
uniform_distribution_params_from_dict = UniformDistributionParams.from_dict(uniform_distribution_params_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


