# gretel_client.rest.ProjectsApi

All URIs are relative to *https://api-dev.gretel.cloud*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_artifact**](ProjectsApi.md#create_artifact) | **POST** /projects/{project_id}/artifacts | Create a new artifact
[**create_invite**](ProjectsApi.md#create_invite) | **POST** /projects/{project_id}/invites | Create a project invite
[**create_model**](ProjectsApi.md#create_model) | **POST** /projects/{project_id}/models | Create and train a new model
[**create_project**](ProjectsApi.md#create_project) | **POST** /projects | 
[**create_record_handler**](ProjectsApi.md#create_record_handler) | **POST** /projects/{project_id}/models/{model_id}/record_handlers | Create a record handler for a model
[**delete_artifact**](ProjectsApi.md#delete_artifact) | **DELETE** /projects/{project_id}/artifacts | Delete an artifact
[**delete_model**](ProjectsApi.md#delete_model) | **DELETE** /projects/{project_id}/models/{model_id} | Delete a model by it&#39;s ID
[**delete_project**](ProjectsApi.md#delete_project) | **DELETE** /projects/{project_id} | 
[**delete_record_handler**](ProjectsApi.md#delete_record_handler) | **DELETE** /projects/{project_id}/models/{model_id}/record_handlers/{record_handler_id} | 
[**download_artifact**](ProjectsApi.md#download_artifact) | **GET** /projects/{project_id}/artifacts/download | 
[**get_artifact_manifest**](ProjectsApi.md#get_artifact_manifest) | **GET** /projects/{project_id}/artifacts/manifest | 
[**get_artifacts**](ProjectsApi.md#get_artifacts) | **GET** /projects/{project_id}/artifacts | List all project artifacts
[**get_model**](ProjectsApi.md#get_model) | **GET** /projects/{project_id}/models/{model_id} | Get model details
[**get_model_artifact**](ProjectsApi.md#get_model_artifact) | **GET** /projects/{project_id}/models/{model_id}/artifact | Get model details
[**get_models**](ProjectsApi.md#get_models) | **GET** /projects/{project_id}/models | List all project models
[**get_project**](ProjectsApi.md#get_project) | **GET** /projects/{project_id} | 
[**get_record_handler**](ProjectsApi.md#get_record_handler) | **GET** /projects/{project_id}/models/{model_id}/record_handlers/{record_handler_id} | Get record handler
[**get_record_handler_artifact**](ProjectsApi.md#get_record_handler_artifact) | **GET** /projects/{project_id}/models/{model_id}/record_handlers/{record_handler_id}/artifact | Get record handler artifact
[**query_record_handlers**](ProjectsApi.md#query_record_handlers) | **GET** /projects/{project_id}/models/{model_id}/record_handlers | Queries record handlers
[**search_projects**](ProjectsApi.md#search_projects) | **GET** /projects | 
[**update_model**](ProjectsApi.md#update_model) | **PATCH** /projects/{project_id}/models/{model_id} | 
[**update_record_handler**](ProjectsApi.md#update_record_handler) | **PATCH** /projects/{project_id}/models/{model_id}/record_handlers/{record_handler_id} | 


# **create_artifact**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} create_artifact(project_id, artifact)

Create a new artifact

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
from gretel_client.rest.model.artifact import Artifact
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    artifact = Artifact(
        filename="filename_example",
    ) # Artifact | 

    # example passing only required values which don't have defaults set
    try:
        # Create a new artifact
        api_response = api_instance.create_artifact(project_id, artifact)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_artifact: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **artifact** | [**Artifact**](Artifact.md)|  |

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Artifact upload details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_invite**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} create_invite(project_id, project_invite)

Create a project invite

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
from gretel_client.rest.model.project_invite import ProjectInvite
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    project_invite = ProjectInvite(
        email="email_example",
        level=1,
    ) # ProjectInvite | 

    # example passing only required values which don't have defaults set
    try:
        # Create a project invite
        api_response = api_instance.create_invite(project_id, project_invite)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_invite: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **project_invite** | [**ProjectInvite**](ProjectInvite.md)|  |

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Project invite details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_model**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} create_model(project_id, body)

Create and train a new model

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    body = {} # {str: (bool, date, datetime, dict, float, int, list, str, none_type)} | 
    dry_run = "dry_run_example" # str | yes or no (optional)
    runner_mode = "cloud" # str |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Create and train a new model
        api_response = api_instance.create_model(project_id, body)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_model: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Create and train a new model
        api_response = api_instance.create_model(project_id, body, dry_run=dry_run, runner_mode=runner_mode)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_model: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**|  |
 **dry_run** | **str**| yes or no | [optional]
 **runner_mode** | **str**|  | [optional]

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Model details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_project**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} create_project()



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
from gretel_client.rest.model.project import Project
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
    api_instance = projects_api.ProjectsApi(api_client)
    project = Project(
        name="name_example",
        display_name="display_name_example",
        description="description_example",
        runner_mode="cloud",
        cluster_guid="cluster_guid_example",
    ) # Project |  (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.create_project(project=project)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_project: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | [**Project**](Project.md)|  | [optional]

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Project details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_record_handler**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} create_record_handler(project_id, model_id)

Create a record handler for a model

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    runner_mode = "cloud" # str |  (optional)
    body = {} # {str: (bool, date, datetime, dict, float, int, list, str, none_type)} |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Create a record handler for a model
        api_response = api_instance.create_record_handler(project_id, model_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_record_handler: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Create a record handler for a model
        api_response = api_instance.create_record_handler(project_id, model_id, runner_mode=runner_mode, body=body)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->create_record_handler: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **runner_mode** | **str**|  | [optional]
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**|  | [optional]

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Record handler details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_artifact**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} delete_artifact(project_id)

Delete an artifact

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    key = "key_example" # str | Artifact key to delete (optional)

    # example passing only required values which don't have defaults set
    try:
        # Delete an artifact
        api_response = api_instance.delete_artifact(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->delete_artifact: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Delete an artifact
        api_response = api_instance.delete_artifact(project_id, key=key)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->delete_artifact: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **key** | **str**| Artifact key to delete | [optional]

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
**200** | Artifact delete confirmation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_model**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} delete_model(project_id, model_id)

Delete a model by it's ID

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id

    # example passing only required values which don't have defaults set
    try:
        # Delete a model by it's ID
        api_response = api_instance.delete_model(project_id, model_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->delete_model: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |

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
**200** | Model delete confirmation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_project**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} delete_project(project_id)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project Id

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.delete_project(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->delete_project: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project Id |

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
**200** | Project delete confirmation. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_record_handler**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} delete_record_handler(project_id, model_id, record_handler_id)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    record_handler_id = "record_handler_id_example" # str | Record handler id

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.delete_record_handler(project_id, model_id, record_handler_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->delete_record_handler: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **record_handler_id** | **str**| Record handler id |

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
**200** | Delete confirmation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **download_artifact**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} download_artifact(project_id)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    key = "key_example" # str | Download artifact by key (optional)
    uncompressed = "false" # str | Return a URL pointing to the uncompressed version of a gzip compressed file (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.download_artifact(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->download_artifact: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.download_artifact(project_id, key=key, uncompressed=uncompressed)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->download_artifact: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **key** | **str**| Download artifact by key | [optional]
 **uncompressed** | **str**| Return a URL pointing to the uncompressed version of a gzip compressed file | [optional]

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
**200** | Download artifact by key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_artifact_manifest**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_artifact_manifest(project_id)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    key = "key_example" # str | Get artifact manifest by key (optional)

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_artifact_manifest(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_artifact_manifest: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.get_artifact_manifest(project_id, key=key)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_artifact_manifest: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **key** | **str**| Get artifact manifest by key | [optional]

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
**200** | Get artifact manifest by key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_artifacts**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_artifacts(project_id)

List all project artifacts

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id

    # example passing only required values which don't have defaults set
    try:
        # List all project artifacts
        api_response = api_instance.get_artifacts(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_artifacts: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |

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
**200** | A list of artifacts |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_model**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_model(project_id, model_id)

Get model details

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    logs = "yes" # str | Deprecated, use `expand` parameter instead. (optional)
    expand = [
        "artifacts",
    ] # [str] |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get model details
        api_response = api_instance.get_model(project_id, model_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_model: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get model details
        api_response = api_instance.get_model(project_id, model_id, logs=logs, expand=expand)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_model: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **logs** | **str**| Deprecated, use &#x60;expand&#x60; parameter instead. | [optional]
 **expand** | **[str]**|  | [optional]

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
**200** | Model details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_model_artifact**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_model_artifact(project_id, model_id, type)

Get model details

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    type = "model" # str | 
    uncompressed = "false" # str | Return a URL pointing to the uncompressed version of a gzip compressed file (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get model details
        api_response = api_instance.get_model_artifact(project_id, model_id, type)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_model_artifact: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get model details
        api_response = api_instance.get_model_artifact(project_id, model_id, type, uncompressed=uncompressed)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_model_artifact: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **type** | **str**|  |
 **uncompressed** | **str**| Return a URL pointing to the uncompressed version of a gzip compressed file | [optional]

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
**200** | Model Artifact details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_models**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_models(project_id)

List all project models

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    limit = 1 # int | Limit number of models to return (optional)
    model_name = "model_name_example" # str | Model name to match on (optional)
    workflow_run_id = "workflow_run_id_example" # str | WorkflowRun ID to match on (optional)
    sort_by = "asc" # str | Direction to sort by. Defaults to \"asc\" (optional)
    sort_field = "last_modified" # str | field to sort on. Defaults to \"last_modified\" (optional)

    # example passing only required values which don't have defaults set
    try:
        # List all project models
        api_response = api_instance.get_models(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_models: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # List all project models
        api_response = api_instance.get_models(project_id, limit=limit, model_name=model_name, workflow_run_id=workflow_run_id, sort_by=sort_by, sort_field=sort_field)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_models: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **limit** | **int**| Limit number of models to return | [optional]
 **model_name** | **str**| Model name to match on | [optional]
 **workflow_run_id** | **str**| WorkflowRun ID to match on | [optional]
 **sort_by** | **str**| Direction to sort by. Defaults to \&quot;asc\&quot; | [optional]
 **sort_field** | **str**| field to sort on. Defaults to \&quot;last_modified\&quot; | [optional]

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
**200** | A list of models |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_project**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_project(project_id)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project Id

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.get_project(project_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_project: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project Id |

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
**200** | Project details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_record_handler**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_record_handler(project_id, model_id, record_handler_id)

Get record handler

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    record_handler_id = "record_handler_id_example" # str | Record handler id
    logs = "yes" # str | Deprecated, use `expand` parameter instead. (optional)
    expand = [
        "artifacts",
    ] # [str] |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get record handler
        api_response = api_instance.get_record_handler(project_id, model_id, record_handler_id)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_record_handler: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get record handler
        api_response = api_instance.get_record_handler(project_id, model_id, record_handler_id, logs=logs, expand=expand)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_record_handler: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **record_handler_id** | **str**| Record handler id |
 **logs** | **str**| Deprecated, use &#x60;expand&#x60; parameter instead. | [optional]
 **expand** | **[str]**|  | [optional]

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
**200** | Record handler details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_record_handler_artifact**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} get_record_handler_artifact(project_id, model_id, record_handler_id, type)

Get record handler artifact

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    record_handler_id = "record_handler_id_example" # str | Record handler id
    type = "run_report_json" # str | 
    uncompressed = "false" # str | Return a URL pointing to the uncompressed version of a gzip compressed file (optional)

    # example passing only required values which don't have defaults set
    try:
        # Get record handler artifact
        api_response = api_instance.get_record_handler_artifact(project_id, model_id, record_handler_id, type)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_record_handler_artifact: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Get record handler artifact
        api_response = api_instance.get_record_handler_artifact(project_id, model_id, record_handler_id, type, uncompressed=uncompressed)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->get_record_handler_artifact: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **record_handler_id** | **str**| Record handler id |
 **type** | **str**|  |
 **uncompressed** | **str**| Return a URL pointing to the uncompressed version of a gzip compressed file | [optional]

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
**200** | Record handler artifact details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **query_record_handlers**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} query_record_handlers(project_id, model_id, status)

Queries record handlers

### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    status = "completed" # str | 
    skip = 1 # int | The number of records being skipped before returning the next set. (optional)
    limit = 1 # int | The number of records returned in each result set. (optional)
    expand = [
        "artifacts",
    ] # [str] |  (optional)

    # example passing only required values which don't have defaults set
    try:
        # Queries record handlers
        api_response = api_instance.query_record_handlers(project_id, model_id, status)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->query_record_handlers: %s\n" % e)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        # Queries record handlers
        api_response = api_instance.query_record_handlers(project_id, model_id, status, skip=skip, limit=limit, expand=expand)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->query_record_handlers: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **status** | **str**|  |
 **skip** | **int**| The number of records being skipped before returning the next set. | [optional]
 **limit** | **int**| The number of records returned in each result set. | [optional]
 **expand** | **[str]**|  | [optional]

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
**200** | Record query results |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **search_projects**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} search_projects()



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    query = "query_example" # str | Project search filters (optional)
    limit = 1 # int | Max number of projects to return (optional)

    # example passing only required values which don't have defaults set
    # and optional values
    try:
        api_response = api_instance.search_projects(query=query, limit=limit)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->search_projects: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **query** | **str**| Project search filters | [optional]
 **limit** | **int**| Max number of projects to return | [optional]

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
**200** | List projects |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_model**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} update_model(project_id, model_id, body)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    body = {} # {str: (bool, date, datetime, dict, float, int, list, str, none_type)} | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.update_model(project_id, model_id, body)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->update_model: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**|  |

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Update confirmation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_record_handler**
> {str: (bool, date, datetime, dict, float, int, list, str, none_type)} update_record_handler(project_id, model_id, record_handler_id, body)



### Example

* Api Key Authentication (ApiKey):
```python
import time
import gretel_client.rest
from gretel_client.rest.api import projects_api
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
    api_instance = projects_api.ProjectsApi(api_client)
    project_id = "project_id_example" # str | Project id
    model_id = "model_id_example" # str | Model id
    record_handler_id = "record_handler_id_example" # str | Record handler id
    body = {} # {str: (bool, date, datetime, dict, float, int, list, str, none_type)} | 

    # example passing only required values which don't have defaults set
    try:
        api_response = api_instance.update_record_handler(project_id, model_id, record_handler_id, body)
        pprint(api_response)
    except gretel_client.rest.ApiException as e:
        print("Exception when calling ProjectsApi->update_record_handler: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_id** | **str**| Project id |
 **model_id** | **str**| Model id |
 **record_handler_id** | **str**| Record handler id |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**|  |

### Return type

**{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**

### Authorization

[ApiKey](../README.md#ApiKey)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Update confirmation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

