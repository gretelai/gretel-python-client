# coding: utf-8

"""
    FastAPI

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

    The version of the OpenAPI document: 0.1.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations

import json
import pprint
import re  # noqa: F401

from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr
from typing_extensions import Self

from gretel_client._api.models.model_config import ModelConfig


class GlobalsInput(BaseModel):
    """
    GlobalsInput
    """  # noqa: E501

    model_configs: Optional[List[ModelConfig]] = None
    model_suite: Optional[StrictStr] = "apache-2.0"
    num_records: Optional[StrictInt] = 100
    __properties: ClassVar[List[str]] = ["model_configs", "model_suite", "num_records"]

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
        """Create an instance of GlobalsInput from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of each item in model_configs (list)
        _items = []
        if self.model_configs:
            for _item in self.model_configs:
                if _item:
                    _items.append(_item.to_dict())
            _dict["model_configs"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of GlobalsInput from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "model_configs": (
                    [ModelConfig.from_dict(_item) for _item in obj["model_configs"]]
                    if obj.get("model_configs") is not None
                    else None
                ),
                "model_suite": (
                    obj.get("model_suite")
                    if obj.get("model_suite") is not None
                    else "apache-2.0"
                ),
                "num_records": (
                    obj.get("num_records")
                    if obj.get("num_records") is not None
                    else 100
                ),
            }
        )
        return _obj
