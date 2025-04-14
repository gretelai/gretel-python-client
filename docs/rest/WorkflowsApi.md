# gretel_client._api.WorkflowsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**check_workflow_run_output**](WorkflowsApi.md#check_workflow_run_output) | **HEAD** /v2/workflows/runs/{workflow_run_id}/output | Workflows Check Run Output
[**compile_workflow_config**](WorkflowsApi.md#compile_workflow_config) | **POST** /v2/workflows/compile | Compile Workflow Config
[**exec_workflow_batch**](WorkflowsApi.md#exec_workflow_batch) | **POST** /v2/workflows/exec_batch | Workflows Exec Batch
[**get_workflow_registry**](WorkflowsApi.md#get_workflow_registry) | **GET** /v2/workflows/registry | Registry
[**retry_exec_workflow_batch**](WorkflowsApi.md#retry_exec_workflow_batch) | **POST** /v2/workflows/exec_batch_retry | Workflows Exec Batch Retry
[**validate_workflow**](WorkflowsApi.md#validate_workflow) | **POST** /v2/workflows/validate | Workflows Validate
[**validate_workflow_task**](WorkflowsApi.md#validate_workflow_task) | **POST** /v2/workflows/tasks/validate | Tasks Validate


# **check_workflow_run_output**
> object check_workflow_run_output(workflow_run_id, type)

Workflows Check Run Output

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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    workflow_run_id = 'workflow_run_id_example' # str | 
    type = 'type_example' # str | 

    try:
        # Workflows Check Run Output
        api_response = api_instance.check_workflow_run_output(workflow_run_id, type)
        print("The response of WorkflowsApi->check_workflow_run_output:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->check_workflow_run_output: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **workflow_run_id** | **str**|  | 
 **type** | **str**|  | 

### Return type

**object**

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

# **compile_workflow_config**
> CompileWorkflowConfigResponse compile_workflow_config(compile_workflow_config_request)

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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    compile_workflow_config_request = gretel_client._api.CompileWorkflowConfigRequest() # CompileWorkflowConfigRequest | 

    try:
        # Compile Workflow Config
        api_response = api_instance.compile_workflow_config(compile_workflow_config_request)
        print("The response of WorkflowsApi->compile_workflow_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->compile_workflow_config: %s\n" % e)
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

# **exec_workflow_batch**
> ExecBatchResponse exec_workflow_batch(exec_batch_request)

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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    exec_batch_request = gretel_client._api.ExecBatchRequest() # ExecBatchRequest | 

    try:
        # Workflows Exec Batch
        api_response = api_instance.exec_workflow_batch(exec_batch_request)
        print("The response of WorkflowsApi->exec_workflow_batch:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->exec_workflow_batch: %s\n" % e)
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

# **get_workflow_registry**
> object get_workflow_registry()

Registry

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
    api_instance = gretel_client._api.WorkflowsApi(api_client)

    try:
        # Registry
        api_response = api_instance.get_workflow_registry()
        print("The response of WorkflowsApi->get_workflow_registry:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->get_workflow_registry: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **retry_exec_workflow_batch**
> ExecBatchResponse retry_exec_workflow_batch(exec_batch_retry_request)

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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    exec_batch_retry_request = gretel_client._api.ExecBatchRetryRequest() # ExecBatchRetryRequest | 

    try:
        # Workflows Exec Batch Retry
        api_response = api_instance.retry_exec_workflow_batch(exec_batch_retry_request)
        print("The response of WorkflowsApi->retry_exec_workflow_batch:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->retry_exec_workflow_batch: %s\n" % e)
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

# **validate_workflow**
> ValidateWorkflowConfigResponse validate_workflow(body)

Workflows Validate

### Example


```python
import gretel_client._api
from gretel_client._api.models.validate_workflow_config_response import ValidateWorkflowConfigResponse
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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    body = None # object | 

    try:
        # Workflows Validate
        api_response = api_instance.validate_workflow(body)
        print("The response of WorkflowsApi->validate_workflow:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->validate_workflow: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **object**|  | 

### Return type

[**ValidateWorkflowConfigResponse**](ValidateWorkflowConfigResponse.md)

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

# **validate_workflow_task**
> TaskValidationResult validate_workflow_task(task_envelope_for_validation)

Tasks Validate

### Example


```python
import gretel_client._api
from gretel_client._api.models.task_envelope_for_validation import TaskEnvelopeForValidation
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
    api_instance = gretel_client._api.WorkflowsApi(api_client)
    task_envelope_for_validation = gretel_client._api.TaskEnvelopeForValidation() # TaskEnvelopeForValidation | 

    try:
        # Tasks Validate
        api_response = api_instance.validate_workflow_task(task_envelope_for_validation)
        print("The response of WorkflowsApi->validate_workflow_task:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling WorkflowsApi->validate_workflow_task: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_envelope_for_validation** | [**TaskEnvelopeForValidation**](TaskEnvelopeForValidation.md)|  | 

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

