# gretel_client.rest.UsersApi

All URIs are relative to *https://api-dev.gretel.cloud*

Method | HTTP request | Description
------------- | ------------- | -------------
[**users_me**](UsersApi.md#users_me) | **GET** /users/me | 


# **users_me**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} users_me()



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import users_api
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
    api_instance = users_api.UsersApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        api_response = api_instance.users_me()
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling UsersApi->users_me: %s\n" % e)
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
**200** | Get my user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

