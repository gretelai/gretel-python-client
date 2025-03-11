# Temperature


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**distribution_type** | [**DistributionType**](DistributionType.md) |  | [optional] 
**params** | [**ManualDistributionParams**](ManualDistributionParams.md) |  | 

## Example

```python
from gretel_client._api.models.temperature import Temperature

# TODO update the JSON string below
json = "{}"
# create an instance of Temperature from a JSON string
temperature_instance = Temperature.from_json(json)
# print the JSON string representation of the object
print(Temperature.to_json())

# convert the object into a dict
temperature_dict = temperature_instance.to_dict()
# create an instance of Temperature from a dict
temperature_from_dict = Temperature.from_dict(temperature_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


