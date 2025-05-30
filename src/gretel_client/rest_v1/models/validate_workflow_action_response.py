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
from typing_extensions import Self


class ValidateWorkflowActionResponse(BaseModel):
    """
    ValidateWorkflowActionResponse
    """  # noqa: E501

    status: Optional[StrictStr] = Field(
        default=None, description="The validation status of the action."
    )
    message: Optional[StrictStr] = Field(
        default=None, description="The error message if the action is invalid."
    )
    __properties: ClassVar[List[str]] = ["status", "message"]

    @field_validator("status")
    def status_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(
            [
                "VALIDATION_STATUS_UNKNOWN",
                "VALIDATION_STATUS_VALIDATING",
                "VALIDATION_STATUS_VALID",
                "VALIDATION_STATUS_INVALID",
            ]
        ):
            raise ValueError(
                "must be one of enum values ('VALIDATION_STATUS_UNKNOWN', 'VALIDATION_STATUS_VALIDATING', 'VALIDATION_STATUS_VALID', 'VALIDATION_STATUS_INVALID')"
            )
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
        """Create an instance of ValidateWorkflowActionResponse from a JSON string"""
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
        """Create an instance of ValidateWorkflowActionResponse from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {"status": obj.get("status"), "message": obj.get("message")}
        )
        return _obj
