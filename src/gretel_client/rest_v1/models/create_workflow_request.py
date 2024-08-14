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

from pydantic import BaseModel, ConfigDict, Field, field_validator, StrictStr
from typing_extensions import Annotated, Self


class CreateWorkflowRequest(BaseModel):
    """
    CreateWorkflowRequest
    """  # noqa: E501

    name: Optional[StrictStr] = Field(
        default=None,
        description="The name of the workflow. This field has been deprecated in favor of the `config.name` field.",
    )
    project_id: Annotated[str, Field(strict=True)] = Field(
        description="The project ID that this workflow should belong to."
    )
    config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The workflow config object. See production documentation for more information on the structure of this field.",
    )
    config_text: Optional[StrictStr] = Field(
        default=None, description="The workflow config as a YAML string."
    )
    runner_mode: Optional[StrictStr] = Field(
        default=None,
        description="The runner mode of the workflow. Can be `cloud` or `hybrid`. Some projects may require workflows to run in a specific mode.",
    )
    __properties: ClassVar[List[str]] = [
        "name",
        "project_id",
        "config",
        "config_text",
        "runner_mode",
    ]

    @field_validator("project_id")
    def project_id_validate_regular_expression(cls, value):
        """Validates the regular expression"""
        if not re.match(r"^proj_.*$", value):
            raise ValueError(r"must validate the regular expression /^proj_.*$/")
        return value

    @field_validator("runner_mode")
    def runner_mode_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(
            [
                "RUNNER_MODE_UNSET",
                "RUNNER_MODE_CLOUD",
                "RUNNER_MODE_HYBRID",
                "RUNNER_MODE_INVALID",
            ]
        ):
            raise ValueError(
                "must be one of enum values ('RUNNER_MODE_UNSET', 'RUNNER_MODE_CLOUD', 'RUNNER_MODE_HYBRID', 'RUNNER_MODE_INVALID')"
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
        """Create an instance of CreateWorkflowRequest from a JSON string"""
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
        """Create an instance of CreateWorkflowRequest from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "name": obj.get("name"),
                "project_id": obj.get("project_id"),
                "config": obj.get("config"),
                "config_text": obj.get("config_text"),
                "runner_mode": obj.get("runner_mode"),
            }
        )
        return _obj
