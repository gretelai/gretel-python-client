# coding: utf-8

"""


No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

The version of the OpenAPI document: 0.0.1
Generated by OpenAPI Generator (https://openapi-generator.tech)

Do not edit the class manually.
"""  # noqa: E501

from __future__ import annotations

import json
import pprint
import re  # noqa: F401

from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator
from typing_extensions import Annotated, Self


class UpdateConnectionRequest(BaseModel):
    """
    Request message for `UpdateConnection`
    """  # noqa: E501

    name: Optional[Annotated[str, Field(min_length=3, strict=True, max_length=30)]] = (
        Field(default=None, description="A new connection name. (optional)")
    )
    credentials: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Plaintext credentials for the connection, to be encrypted in our cloud. This field may only be set if the existing connection's credentials are encrypted with a Gretel-managed key.",
    )
    encrypted_credentials: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pre-encrypted credentials for the connection, encrypted by a customer-managed key. This field may only be set if the existing connection's credentials are encrypted with a user-managed key.",
    )
    config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="An updated connection configuration. See [connection type documentation](https://docs.gretel.ai/create-synthetic-data/workflows-and-connectors/connectors) for structure.",
    )
    connection_target_type: Optional[StrictStr] = Field(
        default=None,
        description="An updated workflow action target type for this connection. Possible values are: `source`, `destination`, `unspecified`",
    )
    auth_strategy: Optional[StrictStr] = Field(
        default=None,
        description="An updated connection auth strategy. See [connection type documentation](https://docs.gretel.ai/create-synthetic-data/workflows-and-connectors/connectors) for possible values.",
    )
    __properties: ClassVar[List[str]] = [
        "name",
        "credentials",
        "encrypted_credentials",
        "config",
        "connection_target_type",
        "auth_strategy",
    ]

    @field_validator("name")
    def name_validate_regular_expression(cls, value):
        """Validates the regular expression"""
        if value is None:
            return value

        if not re.match(r"^[a-z0-9-_]+$", value):
            raise ValueError(r"must validate the regular expression /^[a-z0-9-_]+$/")
        return value

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of UpdateConnectionRequest from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of UpdateConnectionRequest from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "name": obj.get("name"),
                "credentials": obj.get("credentials"),
                "encrypted_credentials": obj.get("encrypted_credentials"),
                "config": obj.get("config"),
                "connection_target_type": obj.get("connection_target_type"),
                "auth_strategy": obj.get("auth_strategy"),
            }
        )
        return _obj
