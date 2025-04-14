# gretel_client._api.FilesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_file**](FilesApi.md#delete_file) | **DELETE** /v1/files/{file_id} | Delete File
[**get_file**](FilesApi.md#get_file) | **GET** /v1/files/{file_id} | Get File
[**list_files**](FilesApi.md#list_files) | **GET** /v1/files | List Files
[**upload_file**](FilesApi.md#upload_file) | **POST** /v1/files | Upload File


# **delete_file**
> FileDeleteResponse delete_file(file_id)

Delete File

### Example


```python
import gretel_client._api
from gretel_client._api.models.file_delete_response import FileDeleteResponse
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.FilesApi(api_client)
    file_id = 'file_id_example' # str | 

    try:
        # Delete File
        api_response = api_instance.delete_file(file_id)
        print("The response of FilesApi->delete_file:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FilesApi->delete_file: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **file_id** | **str**|  | 

### Return type

[**FileDeleteResponse**](FileDeleteResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_file**
> File get_file(file_id)

Get File

### Example


```python
import gretel_client._api
from gretel_client._api.models.file import File
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.FilesApi(api_client)
    file_id = 'file_id_example' # str | 

    try:
        # Get File
        api_response = api_instance.get_file(file_id)
        print("The response of FilesApi->get_file:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FilesApi->get_file: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **file_id** | **str**|  | 

### Return type

[**File**](File.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_files**
> Dict[str, int] list_files(limit=limit, order=order)

List Files

### Example


```python
import gretel_client._api
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.FilesApi(api_client)
    limit = 56 # int |  (optional)
    order = 'order_example' # str |  (optional)

    try:
        # List Files
        api_response = api_instance.list_files(limit=limit, order=order)
        print("The response of FilesApi->list_files:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FilesApi->list_files: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**|  | [optional] 
 **order** | **str**|  | [optional] 

### Return type

**Dict[str, int]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_file**
> File upload_file(file, purpose)

Upload File

### Example


```python
import gretel_client._api
from gretel_client._api.models.file import File
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.FilesApi(api_client)
    file = None # bytearray | 
    purpose = 'purpose_example' # str | 

    try:
        # Upload File
        api_response = api_instance.upload_file(file, purpose)
        print("The response of FilesApi->upload_file:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FilesApi->upload_file: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **file** | **bytearray**|  | 
 **purpose** | **str**|  | 

### Return type

[**File**](File.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

