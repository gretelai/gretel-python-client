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

from pydantic import BaseModel, Field, StrictStr


class CreateWorkflowRunRequest(BaseModel):
    """
    CreateWorkflowRunRequest
    """

    workflow_id: StrictStr = Field(
        ..., description="The ID of the workflow to create a run for."
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="An optional config for the workflow run If provided, this will be used in place of the workflow's config.",
    )
    config_text: Optional[StrictStr] = Field(
        None,
        description="An optional config for the workflow run as a YAML string. If provided, this will be used in place of the workflow's config.",
    )
    __properties = ["workflow_id", "config", "config_text"]

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
    def from_json(cls, json_str: str) -> CreateWorkflowRunRequest:
        """Create an instance of CreateWorkflowRunRequest from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> CreateWorkflowRunRequest:
        """Create an instance of CreateWorkflowRunRequest from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return CreateWorkflowRunRequest.parse_obj(obj)

        _obj = CreateWorkflowRunRequest.parse_obj(
            {
                "workflow_id": obj.get("workflow_id"),
                "config": obj.get("config"),
                "config_text": obj.get("config_text"),
            }
        )
        return _obj
