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
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, field_validator, StrictStr
from typing_extensions import Self

from gretel_client.rest_v1.models.event_component import EventComponent


class ActivityEvent(BaseModel):
    """
    ActivityEvent
    """  # noqa: E501

    occurred_at: datetime
    occured_at: datetime
    subject: EventComponent
    predicate: StrictStr
    object: EventComponent
    status: StrictStr
    __properties: ClassVar[List[str]] = [
        "occurred_at",
        "occured_at",
        "subject",
        "predicate",
        "object",
        "status",
    ]

    @field_validator("predicate")
    def predicate_validate_enum(cls, value):
        """Validates the enum"""
        if value not in set(["PREDICATE_CREATED_AT"]):
            raise ValueError("must be one of enum values ('PREDICATE_CREATED_AT')")
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
        """Create an instance of ActivityEvent from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of subject
        if self.subject:
            _dict["subject"] = self.subject.to_dict()
        # override the default output from pydantic by calling `to_dict()` of object
        if self.object:
            _dict["object"] = self.object.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of ActivityEvent from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "occurred_at": obj.get("occurred_at"),
                "occured_at": obj.get("occured_at"),
                "subject": (
                    EventComponent.from_dict(obj["subject"])
                    if obj.get("subject") is not None
                    else None
                ),
                "predicate": obj.get("predicate"),
                "object": (
                    EventComponent.from_dict(obj["object"])
                    if obj.get("object") is not None
                    else None
                ),
                "status": obj.get("status"),
            }
        )
        return _obj
