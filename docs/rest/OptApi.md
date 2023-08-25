# gretel_client.rest.OptApi

All URIs are relative to *https://api-dev.gretel.cloud*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_container_login**](OptApi.md#get_container_login) | **GET** /opt/containers/get_login | Get Gretel container Docker login credentials
[**get_licenses**](OptApi.md#get_licenses) | **GET** /opt/licenses | Get Gretel Backend/Worker licenses
[**get_model_credentials**](OptApi.md#get_model_credentials) | **GET** /opt/models/get_credentials | Get Gretel models fetching credentials


# **get_container_login**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_container_login()

Get Gretel container Docker login credentials

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import opt_api
from pprint import pprint
# Defining the host is optional and defaults to https://api-dev.gretel.cloud
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client.rest.Configuration(
    host = "https://api-dev.gretel.cloud"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: ApiKey
configuration.api_key['ApiKey'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['ApiKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = opt_api.OptApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Get Gretel container Docker login credentials
        api_response = api_instance.get_container_login()
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling OptApi->get_container_login: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Docker login credentials |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_licenses**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_licenses()

Get Gretel Backend/Worker licenses

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import opt_api
from pprint import pprint
# Defining the host is optional and defaults to https://api-dev.gretel.cloud
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client.rest.Configuration(
    host = "https://api-dev.gretel.cloud"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: ApiKey
configuration.api_key['ApiKey'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['ApiKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = opt_api.OptApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Get Gretel Backend/Worker licenses
        api_response = api_instance.get_licenses()
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling OptApi->get_licenses: %s\n" % e)
```


### Parameters
This endpoint does not need any parameter.

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Licenses |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_model_credentials**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_model_credentials(uid)

Get Gretel models fetching credentials

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import opt_api
from pprint import pprint
# Defining the host is optional and defaults to https://api-dev.gretel.cloud
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client.rest.Configuration(
    host = "https://api-dev.gretel.cloud"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: ApiKey
configuration.api_key['ApiKey'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['ApiKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client.rest.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = opt_api.OptApi(api_client)
    uid = "uid_example" # str | 
    type = "train" # str |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get Gretel models fetching credentials
        api_response = api_instance.get_model_credentials(uid)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling OptApi->get_model_credentials: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get Gretel models fetching credentials
        api_response = api_instance.get_model_credentials(uid, type=type)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling OptApi->get_model_credentials: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **uid** | **str**|  |
 **type** | **str**|  | [optional]

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Models fetching credentials |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

