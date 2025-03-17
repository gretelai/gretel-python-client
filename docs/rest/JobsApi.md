# gretel_client.rest.JobsApi

All URIs are relative to *https://api-dev.gretel.cloud*

Method | HTTP request | Description
------------- | ------------- | -------------
[**receive_one**](JobsApi.md#receive_one) | **POST** /jobs/receive_one | Get Gretel job for scheduling


# **receive_one**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} receive_one()

Get Gretel job for scheduling

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import jobs_api
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
    api_instance = jobs_api.JobsApi(api_client)
    project_id = "project_id_example" # str | Deprecated, use project_ids instead. (optional)
    project_ids = [
        "project_ids_example",
    ] # [str] |  (optional)
    runner_modes = [
        "cloud",
    ] # [str] |  (optional)
    org_only = True # bool | Query for jobs within the same organization only (optional)
    cluster_guid = "cluster_guid_example" # str | GUID of the cluster for which to retrieve jobs (optional)
    use_combined_models_image = False # bool | True results in the jobs' container_image field being set to the combined models image (optional) if omitted the server will use the default value of False

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get Gretel job for scheduling
        api_response = api_instance.receive_one(project_id=project_id, project_ids=project_ids, runner_modes=runner_modes, org_only=org_only, cluster_guid=cluster_guid, use_combined_models_image=use_combined_models_image)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling JobsApi->receive_one: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Deprecated, use project_ids instead. | [optional]
 **project_ids** | **[str]**|  | [optional]
 **runner_modes** | **[str]**|  | [optional]
 **org_only** | **bool**| Query for jobs within the same organization only | [optional]
 **cluster_guid** | **str**| GUID of the cluster for which to retrieve jobs | [optional]
 **use_combined_models_image** | **bool**| True results in the jobs&#39; container_image field being set to the combined models image | [optional] if omitted the server will use the default value of False

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
**200** | Job to schedule |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

