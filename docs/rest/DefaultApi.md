# gretel_client._api.DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**compile_workflow_config_v2_workflows_compile_post**](DefaultApi.md#compile_workflow_config_v2_workflows_compile_post) | **POST** /v2/workflows/compile | Compile Workflow Config
[**delete_file_v1_files_file_id_delete**](DefaultApi.md#delete_file_v1_files_file_id_delete) | **DELETE** /v1/files/{file_id} | Delete File
[**get_file_v1_files_file_id_get**](DefaultApi.md#get_file_v1_files_file_id_get) | **GET** /v1/files/{file_id} | Get File
[**list_files_v1_files_get**](DefaultApi.md#list_files_v1_files_get) | **GET** /v1/files | List Files
[**registry_v2_workflows_registry_get**](DefaultApi.md#registry_v2_workflows_registry_get) | **GET** /v2/workflows/registry | Registry
[**tasks_validate_v2_workflows_tasks_validate_post**](DefaultApi.md#tasks_validate_v2_workflows_tasks_validate_post) | **POST** /v2/workflows/tasks/validate | Tasks Validate
[**upload_file_v1_files_post**](DefaultApi.md#upload_file_v1_files_post) | **POST** /v1/files | Upload File
[**workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post**](DefaultApi.md#workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post) | **POST** /v2/workflows/exec_batch_retry | Workflows Exec Batch Retry
[**workflows_exec_batch_v2_workflows_exec_batch_post**](DefaultApi.md#workflows_exec_batch_v2_workflows_exec_batch_post) | **POST** /v2/workflows/exec_batch | Workflows Exec Batch
[**workflows_validate_v2_workflows_validate_post**](DefaultApi.md#workflows_validate_v2_workflows_validate_post) | **POST** /v2/workflows/validate | Workflows Validate


# **compile_workflow_config_v2_workflows_compile_post**
> CompileWorkflowConfigResponse compile_workflow_config_v2_workflows_compile_post(compile_workflow_config_request)

Compile Workflow Config

### Example


```python
import gretel_client._api
from gretel_client._api.models.compile_workflow_config_request import CompileWorkflowConfigRequest
from gretel_client._api.models.compile_workflow_config_response import CompileWorkflowConfigResponse
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
    api_instance = gretel_client._api.DefaultApi(api_client)
    compile_workflow_config_request = gretel_client._api.CompileWorkflowConfigRequest() # CompileWorkflowConfigRequest | 

    try:
        # Compile Workflow Config
        api_response = api_instance.compile_workflow_config_v2_workflows_compile_post(compile_workflow_config_request)
        print("The response of DefaultApi->compile_workflow_config_v2_workflows_compile_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->compile_workflow_config_v2_workflows_compile_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **compile_workflow_config_request** | [**CompileWorkflowConfigRequest**](CompileWorkflowConfigRequest.md)|  | 

### Return type

[**CompileWorkflowConfigResponse**](CompileWorkflowConfigResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_file_v1_files_file_id_delete**
> FileDeleteResponse delete_file_v1_files_file_id_delete(file_id)

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
    api_instance = gretel_client._api.DefaultApi(api_client)
    file_id = 'file_id_example' # str | 

    try:
        # Delete File
        api_response = api_instance.delete_file_v1_files_file_id_delete(file_id)
        print("The response of DefaultApi->delete_file_v1_files_file_id_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->delete_file_v1_files_file_id_delete: %s\n" % e)
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

# **get_file_v1_files_file_id_get**
> File get_file_v1_files_file_id_get(file_id)

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
    api_instance = gretel_client._api.DefaultApi(api_client)
    file_id = 'file_id_example' # str | 

    try:
        # Get File
        api_response = api_instance.get_file_v1_files_file_id_get(file_id)
        print("The response of DefaultApi->get_file_v1_files_file_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_file_v1_files_file_id_get: %s\n" % e)
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

# **list_files_v1_files_get**
> Dict[str, int] list_files_v1_files_get(limit=limit, order=order)

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
    api_instance = gretel_client._api.DefaultApi(api_client)
    limit = 56 # int |  (optional)
    order = 'order_example' # str |  (optional)

    try:
        # List Files
        api_response = api_instance.list_files_v1_files_get(limit=limit, order=order)
        print("The response of DefaultApi->list_files_v1_files_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->list_files_v1_files_get: %s\n" % e)
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

# **registry_v2_workflows_registry_get**
> object registry_v2_workflows_registry_get()

Registry

### Example

* Api Key Authentication (GretelAPIKey):

```python
import gretel_client._api
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: GretelAPIKey
configuration.api_key['GretelAPIKey'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['GretelAPIKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.DefaultApi(api_client)

    try:
        # Registry
        api_response = api_instance.registry_v2_workflows_registry_get()
        print("The response of DefaultApi->registry_v2_workflows_registry_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->registry_v2_workflows_registry_get: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**object**

### Authorization

[GretelAPIKey](../README.md#GretelAPIKey)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **tasks_validate_v2_workflows_tasks_validate_post**
> TaskValidationResult tasks_validate_v2_workflows_tasks_validate_post(task_envelope)

Tasks Validate

### Example


```python
import gretel_client._api
from gretel_client._api.models.task_envelope import TaskEnvelope
from gretel_client._api.models.task_validation_result import TaskValidationResult
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
    api_instance = gretel_client._api.DefaultApi(api_client)
    task_envelope = gretel_client._api.TaskEnvelope() # TaskEnvelope | 

    try:
        # Tasks Validate
        api_response = api_instance.tasks_validate_v2_workflows_tasks_validate_post(task_envelope)
        print("The response of DefaultApi->tasks_validate_v2_workflows_tasks_validate_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->tasks_validate_v2_workflows_tasks_validate_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_envelope** | [**TaskEnvelope**](TaskEnvelope.md)|  | 

### Return type

[**TaskValidationResult**](TaskValidationResult.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_file_v1_files_post**
> File upload_file_v1_files_post(file, purpose)

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
    api_instance = gretel_client._api.DefaultApi(api_client)
    file = None # bytearray | 
    purpose = 'purpose_example' # str | 

    try:
        # Upload File
        api_response = api_instance.upload_file_v1_files_post(file, purpose)
        print("The response of DefaultApi->upload_file_v1_files_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->upload_file_v1_files_post: %s\n" % e)
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

# **workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post**
> ExecBatchResponse workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post(exec_batch_retry_request)

Workflows Exec Batch Retry

### Example

* Api Key Authentication (GretelAPIKey):

```python
import gretel_client._api
from gretel_client._api.models.exec_batch_response import ExecBatchResponse
from gretel_client._api.models.exec_batch_retry_request import ExecBatchRetryRequest
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: GretelAPIKey
configuration.api_key['GretelAPIKey'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['GretelAPIKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.DefaultApi(api_client)
    exec_batch_retry_request = gretel_client._api.ExecBatchRetryRequest() # ExecBatchRetryRequest | 

    try:
        # Workflows Exec Batch Retry
        api_response = api_instance.workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post(exec_batch_retry_request)
        print("The response of DefaultApi->workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->workflows_exec_batch_retry_v2_workflows_exec_batch_retry_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **exec_batch_retry_request** | [**ExecBatchRetryRequest**](ExecBatchRetryRequest.md)|  | 

### Return type

[**ExecBatchResponse**](ExecBatchResponse.md)

### Authorization

[GretelAPIKey](../README.md#GretelAPIKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **workflows_exec_batch_v2_workflows_exec_batch_post**
> ExecBatchResponse workflows_exec_batch_v2_workflows_exec_batch_post(exec_batch_request)

Workflows Exec Batch

### Example

* Api Key Authentication (GretelAPIKey):

```python
import gretel_client._api
from gretel_client._api.models.exec_batch_request import ExecBatchRequest
from gretel_client._api.models.exec_batch_response import ExecBatchResponse
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: GretelAPIKey
configuration.api_key['GretelAPIKey'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['GretelAPIKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.DefaultApi(api_client)
    exec_batch_request = gretel_client._api.ExecBatchRequest() # ExecBatchRequest | 

    try:
        # Workflows Exec Batch
        api_response = api_instance.workflows_exec_batch_v2_workflows_exec_batch_post(exec_batch_request)
        print("The response of DefaultApi->workflows_exec_batch_v2_workflows_exec_batch_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->workflows_exec_batch_v2_workflows_exec_batch_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **exec_batch_request** | [**ExecBatchRequest**](ExecBatchRequest.md)|  | 

### Return type

[**ExecBatchResponse**](ExecBatchResponse.md)

### Authorization

[GretelAPIKey](../README.md#GretelAPIKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **workflows_validate_v2_workflows_validate_post**
> ValidateWorkflowConfigResponse workflows_validate_v2_workflows_validate_post(workflow_input)

Workflows Validate

### Example

* Api Key Authentication (GretelAPIKey):

```python
import gretel_client._api
from gretel_client._api.models.validate_workflow_config_response import ValidateWorkflowConfigResponse
from gretel_client._api.models.workflow_input import WorkflowInput
from gretel_client._api.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = gretel_client._api.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: GretelAPIKey
configuration.api_key['GretelAPIKey'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['GretelAPIKey'] = 'Bearer'

# Enter a context with an instance of the API client
with gretel_client._api.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = gretel_client._api.DefaultApi(api_client)
    workflow_input = gretel_client._api.WorkflowInput() # WorkflowInput | 

    try:
        # Workflows Validate
        api_response = api_instance.workflows_validate_v2_workflows_validate_post(workflow_input)
        print("The response of DefaultApi->workflows_validate_v2_workflows_validate_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->workflows_validate_v2_workflows_validate_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **workflow_input** | [**WorkflowInput**](WorkflowInput.md)|  | 

### Return type

[**ValidateWorkflowConfigResponse**](ValidateWorkflowConfigResponse.md)

### Authorization

[GretelAPIKey](../README.md#GretelAPIKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

