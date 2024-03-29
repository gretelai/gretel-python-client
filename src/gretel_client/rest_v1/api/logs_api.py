# coding: utf-8

"""
    

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import io
import re  # noqa: F401
import warnings

from typing import Optional

from pydantic import StrictInt, StrictStr, validate_arguments, ValidationError
from typing_extensions import Annotated

from gretel_client.rest_v1.api_client import ApiClient
from gretel_client.rest_v1.api_response import ApiResponse
from gretel_client.rest_v1.exceptions import ApiTypeError, ApiValueError  # noqa: F401
from gretel_client.rest_v1.models.get_log_response import GetLogResponse
from gretel_client.rest_v1.models.get_log_upload_url_response import (
    GetLogUploadURLResponse,
)


class LogsApi(object):
    """NOTE: This class is auto generated by OpenAPI Generator
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient.get_default()
        self.api_client = api_client

    @validate_arguments
    def get_log_upload_url(
        self,
        workflow_run_id: StrictStr,
        action_name: StrictStr,
        workflow_task_id: StrictStr,
        **kwargs,
    ) -> GetLogUploadURLResponse:  # noqa: E501
        """get_log_upload_url  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_log_upload_url(workflow_run_id, action_name, workflow_task_id, async_req=True)
        >>> result = thread.get()

        :param workflow_run_id: (required)
        :type workflow_run_id: str
        :param action_name: (required)
        :type action_name: str
        :param workflow_task_id: (required)
        :type workflow_task_id: str
        :param async_req: Whether to execute the request asynchronously.
        :type async_req: bool, optional
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :return: Returns the result object.
                 If the method is called asynchronously,
                 returns the request thread.
        :rtype: GetLogUploadURLResponse
        """
        kwargs["_return_http_data_only"] = True
        if "_preload_content" in kwargs:
            raise ValueError(
                "Error! Please call the get_log_upload_url_with_http_info method with `_preload_content` instead and obtain raw data from ApiResponse.raw_data"
            )
        return self.get_log_upload_url_with_http_info(
            workflow_run_id, action_name, workflow_task_id, **kwargs
        )  # noqa: E501

    @validate_arguments
    def get_log_upload_url_with_http_info(
        self,
        workflow_run_id: StrictStr,
        action_name: StrictStr,
        workflow_task_id: StrictStr,
        **kwargs,
    ) -> ApiResponse:  # noqa: E501
        """get_log_upload_url  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_log_upload_url_with_http_info(workflow_run_id, action_name, workflow_task_id, async_req=True)
        >>> result = thread.get()

        :param workflow_run_id: (required)
        :type workflow_run_id: str
        :param action_name: (required)
        :type action_name: str
        :param workflow_task_id: (required)
        :type workflow_task_id: str
        :param async_req: Whether to execute the request asynchronously.
        :type async_req: bool, optional
        :param _preload_content: if False, the ApiResponse.data will
                                 be set to none and raw_data will store the
                                 HTTP response body without reading/decoding.
                                 Default is True.
        :type _preload_content: bool, optional
        :param _return_http_data_only: response data instead of ApiResponse
                                       object with status code, headers, etc
        :type _return_http_data_only: bool, optional
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the authentication
                              in the spec for a single request.
        :type _request_auth: dict, optional
        :type _content_type: string, optional: force content-type for the request
        :return: Returns the result object.
                 If the method is called asynchronously,
                 returns the request thread.
        :rtype: tuple(GetLogUploadURLResponse, status_code(int), headers(HTTPHeaderDict))
        """

        _params = locals()

        _all_params = ["workflow_run_id", "action_name", "workflow_task_id"]
        _all_params.extend(
            [
                "async_req",
                "_return_http_data_only",
                "_preload_content",
                "_request_timeout",
                "_request_auth",
                "_content_type",
                "_headers",
            ]
        )

        # validate the arguments
        for _key, _val in _params["kwargs"].items():
            if _key not in _all_params:
                raise ApiTypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method get_log_upload_url" % _key
                )
            _params[_key] = _val
        del _params["kwargs"]

        _collection_formats = {}

        # process the path parameters
        _path_params = {}
        if _params["workflow_run_id"]:
            _path_params["workflow_run_id"] = _params["workflow_run_id"]

        if _params["action_name"]:
            _path_params["action_name"] = _params["action_name"]

        if _params["workflow_task_id"]:
            _path_params["workflow_task_id"] = _params["workflow_task_id"]

        # process the query parameters
        _query_params = []
        # process the header parameters
        _header_params = dict(_params.get("_headers", {}))
        # process the form parameters
        _form_params = []
        _files = {}
        # process the body parameter
        _body_params = None
        # set the HTTP header `Accept`
        _header_params["Accept"] = self.api_client.select_header_accept(
            ["application/json"]
        )  # noqa: E501

        # authentication setting
        _auth_settings = []  # noqa: E501

        _response_types_map = {
            "200": "GetLogUploadURLResponse",
        }

        return self.api_client.call_api(
            "/v1/logs/workflowruns/{workflow_run_id}/actions/{action_name}/tasks/{workflow_task_id}",
            "GET",
            _path_params,
            _query_params,
            _header_params,
            body=_body_params,
            post_params=_form_params,
            files=_files,
            response_types_map=_response_types_map,
            auth_settings=_auth_settings,
            async_req=_params.get("async_req"),
            _return_http_data_only=_params.get("_return_http_data_only"),  # noqa: E501
            _preload_content=_params.get("_preload_content", True),
            _request_timeout=_params.get("_request_timeout"),
            collection_formats=_collection_formats,
            _request_auth=_params.get("_request_auth"),
        )

    @validate_arguments
    def get_logs(
        self,
        query: Optional[StrictStr] = None,
        limit: Optional[StrictInt] = None,
        page_token: Optional[StrictStr] = None,
        **kwargs,
    ) -> GetLogResponse:  # noqa: E501
        """get_logs  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_logs(query, limit, page_token, async_req=True)
        >>> result = thread.get()

        :param query:
        :type query: str
        :param limit:
        :type limit: int
        :param page_token:
        :type page_token: str
        :param async_req: Whether to execute the request asynchronously.
        :type async_req: bool, optional
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :return: Returns the result object.
                 If the method is called asynchronously,
                 returns the request thread.
        :rtype: GetLogResponse
        """
        kwargs["_return_http_data_only"] = True
        if "_preload_content" in kwargs:
            raise ValueError(
                "Error! Please call the get_logs_with_http_info method with `_preload_content` instead and obtain raw data from ApiResponse.raw_data"
            )
        return self.get_logs_with_http_info(
            query, limit, page_token, **kwargs
        )  # noqa: E501

    @validate_arguments
    def get_logs_with_http_info(
        self,
        query: Optional[StrictStr] = None,
        limit: Optional[StrictInt] = None,
        page_token: Optional[StrictStr] = None,
        **kwargs,
    ) -> ApiResponse:  # noqa: E501
        """get_logs  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_logs_with_http_info(query, limit, page_token, async_req=True)
        >>> result = thread.get()

        :param query:
        :type query: str
        :param limit:
        :type limit: int
        :param page_token:
        :type page_token: str
        :param async_req: Whether to execute the request asynchronously.
        :type async_req: bool, optional
        :param _preload_content: if False, the ApiResponse.data will
                                 be set to none and raw_data will store the
                                 HTTP response body without reading/decoding.
                                 Default is True.
        :type _preload_content: bool, optional
        :param _return_http_data_only: response data instead of ApiResponse
                                       object with status code, headers, etc
        :type _return_http_data_only: bool, optional
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the authentication
                              in the spec for a single request.
        :type _request_auth: dict, optional
        :type _content_type: string, optional: force content-type for the request
        :return: Returns the result object.
                 If the method is called asynchronously,
                 returns the request thread.
        :rtype: tuple(GetLogResponse, status_code(int), headers(HTTPHeaderDict))
        """

        _params = locals()

        _all_params = ["query", "limit", "page_token"]
        _all_params.extend(
            [
                "async_req",
                "_return_http_data_only",
                "_preload_content",
                "_request_timeout",
                "_request_auth",
                "_content_type",
                "_headers",
            ]
        )

        # validate the arguments
        for _key, _val in _params["kwargs"].items():
            if _key not in _all_params:
                raise ApiTypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method get_logs" % _key
                )
            _params[_key] = _val
        del _params["kwargs"]

        _collection_formats = {}

        # process the path parameters
        _path_params = {}

        # process the query parameters
        _query_params = []
        if _params.get("query") is not None:  # noqa: E501
            _query_params.append(("query", _params["query"]))

        if _params.get("limit") is not None:  # noqa: E501
            _query_params.append(("limit", _params["limit"]))

        if _params.get("page_token") is not None:  # noqa: E501
            _query_params.append(("page_token", _params["page_token"]))

        # process the header parameters
        _header_params = dict(_params.get("_headers", {}))
        # process the form parameters
        _form_params = []
        _files = {}
        # process the body parameter
        _body_params = None
        # set the HTTP header `Accept`
        _header_params["Accept"] = self.api_client.select_header_accept(
            ["application/json"]
        )  # noqa: E501

        # authentication setting
        _auth_settings = []  # noqa: E501

        _response_types_map = {
            "200": "GetLogResponse",
        }

        return self.api_client.call_api(
            "/v1/logs",
            "GET",
            _path_params,
            _query_params,
            _header_params,
            body=_body_params,
            post_params=_form_params,
            files=_files,
            response_types_map=_response_types_map,
            auth_settings=_auth_settings,
            async_req=_params.get("async_req"),
            _return_http_data_only=_params.get("_return_http_data_only"),  # noqa: E501
            _preload_content=_params.get("_preload_content", True),
            _request_timeout=_params.get("_request_timeout"),
            collection_formats=_collection_formats,
            _request_auth=_params.get("_request_auth"),
        )
