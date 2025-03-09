# File


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**bytes** | **int** |  | 
**created_at** | **int** |  | 
**file_id** | **str** |  | 
**filename** | **str** |  | 
**object** | **str** |  | 
**purpose** | **str** |  | 

## Example

```python
from gretel_client._api.models.file import File

# TODO update the JSON string below
json = "{}"
# create an instance of File from a JSON string
file_instance = File.from_json(json)
# print the JSON string representation of the object
print(File.to_json())

# convert the object into a dict
file_dict = file_instance.to_dict()
# create an instance of File from a dict
file_from_dict = File.from_dict(file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


