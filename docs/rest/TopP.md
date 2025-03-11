# TopP


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**distribution_type** | [**DistributionType**](DistributionType.md) |  | [optional] 
**params** | [**ManualDistributionParams**](ManualDistributionParams.md) |  | 

## Example

```python
from gretel_client._api.models.top_p import TopP

# TODO update the JSON string below
json = "{}"
# create an instance of TopP from a JSON string
top_p_instance = TopP.from_json(json)
# print the JSON string representation of the object
print(TopP.to_json())

# convert the object into a dict
top_p_dict = top_p_instance.to_dict()
# create an instance of TopP from a dict
top_p_from_dict = TopP.from_dict(top_p_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


