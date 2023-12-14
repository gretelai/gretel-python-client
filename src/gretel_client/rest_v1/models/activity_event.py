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

from datetime import datetime

from pydantic import BaseModel, Field, StrictStr, validator

from gretel_client.rest_v1.models.event_component import EventComponent


class ActivityEvent(BaseModel):
    """
    ActivityEvent
    """

    occurred_at: datetime = Field(...)
    occured_at: datetime = Field(...)
    subject: EventComponent = Field(...)
    predicate: StrictStr = Field(...)
    object: EventComponent = Field(...)
    status: StrictStr = Field(...)
    __properties = [
        "occurred_at",
        "occured_at",
        "subject",
        "predicate",
        "object",
        "status",
    ]

    @validator("predicate")
    def predicate_validate_enum(cls, value):
        """Validates the enum"""
        if value not in ("PREDICATE_CREATED_AT"):
            raise ValueError("must be one of enum values ('PREDICATE_CREATED_AT')")
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
    def from_json(cls, json_str: str) -> ActivityEvent:
        """Create an instance of ActivityEvent from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        # override the default output from pydantic by calling `to_dict()` of subject
        if self.subject:
            _dict["subject"] = self.subject.to_dict()
        # override the default output from pydantic by calling `to_dict()` of object
        if self.object:
            _dict["object"] = self.object.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> ActivityEvent:
        """Create an instance of ActivityEvent from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return ActivityEvent.parse_obj(obj)

        _obj = ActivityEvent.parse_obj(
            {
                "occurred_at": obj.get("occurred_at"),
                "occured_at": obj.get("occured_at"),
                "subject": EventComponent.from_dict(obj.get("subject"))
                if obj.get("subject") is not None
                else None,
                "predicate": obj.get("predicate"),
                "object": EventComponent.from_dict(obj.get("object"))
                if obj.get("object") is not None
                else None,
                "status": obj.get("status"),
            }
        )
        return _obj
