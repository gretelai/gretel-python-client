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

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, StrictStr, validator


class ValidateWorkflowConfigRequest(BaseModel):
    """
    ValidateWorkflowConfigRequest
    """

    config: Dict[str, Any] = Field(...)
    runner_mode: Optional[StrictStr] = None
    __properties = ["config", "runner_mode"]

    @validator("runner_mode")
    def runner_mode_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in (
            "RUNNER_MODE_UNSET",
            "RUNNER_MODE_CLOUD",
            "RUNNER_MODE_HYBRID",
            "RUNNER_MODE_INVALID",
        ):
            raise ValueError(
                "must be one of enum values ('RUNNER_MODE_UNSET', 'RUNNER_MODE_CLOUD', 'RUNNER_MODE_HYBRID', 'RUNNER_MODE_INVALID')"
            )
        return value

    class Config:
        """Pydantic configuration"""

        allow_population_by_field_name = True
        validate_assignment = True

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.dict(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> ValidateWorkflowConfigRequest:
        """Create an instance of ValidateWorkflowConfigRequest from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> ValidateWorkflowConfigRequest:
        """Create an instance of ValidateWorkflowConfigRequest from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return ValidateWorkflowConfigRequest.parse_obj(obj)

        _obj = ValidateWorkflowConfigRequest.parse_obj(
            {"config": obj.get("config"), "runner_mode": obj.get("runner_mode")}
        )
        return _obj