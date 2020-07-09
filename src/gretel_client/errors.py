"""
Custom Exceptions
"""
import json


class ClientError(Exception):
    """Generic exception that can be thrown
    """

    pass


class BadRequest(ClientError):
    """A custom error class that can be raised when a non-200 OK
    is received back from the Gretel API.

    The Gretel API returns errors with the following format::

        {
            "message": "a description of what is wrong"
            "context": {}
        }

    The ``context`` key will contain structured information about
    a particular field if there was an error with it.
    """

    def __init__(self, msg: dict):
        """Create an API exception.

        Args:
            msg: A dictionary that is created from the JSON payload
                returned by the Gretel API
        """
        self._msg_dict = msg
        self.message = self._msg_dict["message"]
        """A human readable description of what went wrong """

        self.context = self._msg_dict["context"]
        """A dictionary that contains field-specific errors from the
        request that was made.

        Example POST payload with invalid field::

            {'foo': ['Unknown field.']}
        """

    def __str__(self):  # pragma: no cover
        return self.message

    def as_str(self):
        """Return the error as a single string by combining the message
        and serializing the ``context`` to a JSON string.

        Example::

            'Invalid JSON Payload: {"foo": ["Unknown field."]}'
        """
        return f"{self.message}: {json.dumps(self.context)}"


class NotFound(BadRequest):
    """HTTP 404"""
    pass


class Unauthorized(Exception):
    """Represents a 401 that gets returned
    from the Gretel API authorizer. Because this
    is directly handled by our authorizer Lambdas,
    we get a different body back (missing the ``context``)
    object.
    """

    def __init__(self, msg: dict):
        self._msg_dict = msg

        self.message = self._msg_dict["message"]
        """The message string returned from the API
        """

    def __str__(self):
        return self.message


class Forbidden(Exception):
    """HTTP 403. The only time this is returned
    is during rate limiting of API calls
    """
    pass
