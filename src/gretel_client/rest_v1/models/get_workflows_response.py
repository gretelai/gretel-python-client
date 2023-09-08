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

from typing import List, Optional

from pydantic import BaseModel, conlist

from gretel_client.rest_v1.models.workflow import Workflow


class GetWorkflowsResponse(BaseModel):
    """
    GetWorkflowsResponse
    """

    workflows: Optional[conlist(Workflow)] = None
    __properties = ["workflows"]

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
    def from_json(cls, json_str: str) -> GetWorkflowsResponse:
        """Create an instance of GetWorkflowsResponse from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        # override the default output from pydantic by calling `to_dict()` of each item in workflows (list)
        _items = []
        if self.workflows:
            for _item in self.workflows:
                if _item:
                    _items.append(_item.to_dict())
            _dict["workflows"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> GetWorkflowsResponse:
        """Create an instance of GetWorkflowsResponse from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return GetWorkflowsResponse.parse_obj(obj)

        _obj = GetWorkflowsResponse.parse_obj(
            {
                "workflows": [
                    Workflow.from_dict(_item) for _item in obj.get("workflows")
                ]
                if obj.get("workflows") is not None
                else None
            }
        )
        return _obj
