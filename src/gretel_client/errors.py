"""
Common Gretel SDK errors and exceptions
"""

import requests

BROKEN_RESPONSE_STREAM_ERROR_MESSAGE = (
    "Error consuming API response stream. "
    "This error is likely temporary and we recommend retrying your request. "
    "If this problem persists please contact support."
)


class NavigatorApiError(Exception):
    """
    Base error type for all errors related to
    communicating with the API.
    """


class NavigatorApiClientError(NavigatorApiError):
    """
    Error type for 4xx error responses from the API.
    """


class NavigatorApiServerError(NavigatorApiError):
    """
    Error type for 5xx error responses from the API.
    """


class NavigatorApiStreamingResponseError(NavigatorApiError):
    """
    Error type for issues encountered while handling a
    streaming response from the API, such as it being
    incomplete or malformed.
    """


def check_for_error_response(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError:
        if 400 <= response.status_code < 500:
            raise NavigatorApiClientError(response.json())
        else:
            raise NavigatorApiServerError(response.json())
