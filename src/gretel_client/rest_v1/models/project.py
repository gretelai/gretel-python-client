# coding: utf-8

"""
    

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)  # noqa: E501

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""


from __future__ import annotations

import json
import pprint
import re  # noqa: F401

from datetime import datetime
from inspect import getfullargspec
from typing import Optional

from pydantic import BaseModel, StrictBool, StrictStr


class Project(BaseModel):
    """
    Project
    """

    id: Optional[StrictStr] = None
    uid: Optional[StrictStr] = None
    name: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    long_description: Optional[StrictStr] = None
    owner: Optional[StrictStr] = None
    color: Optional[StrictStr] = None
    public: Optional[StrictBool] = None
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    __properties = [
        "id",
        "uid",
        "name",
        "description",
        "long_description",
        "owner",
        "color",
        "public",
        "modified",
        "created",
    ]

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.dict(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Project:
        """Create an instance of Project from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> Project:
        """Create an instance of Project from a dict"""
        if obj is None:
            return None

        if type(obj) is not dict:
            return Project.parse_obj(obj)

        _obj = Project.parse_obj(
            {
                "id": obj.get("id"),
                "uid": obj.get("uid"),
                "name": obj.get("name"),
                "description": obj.get("description"),
                "long_description": obj.get("long_description"),
                "owner": obj.get("owner"),
                "color": obj.get("color"),
                "public": obj.get("public"),
                "modified": obj.get("modified"),
                "created": obj.get("created"),
            }
        )
        return _obj
