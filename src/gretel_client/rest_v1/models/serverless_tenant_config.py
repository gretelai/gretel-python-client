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

from pydantic import BaseModel, ConfigDict, Field, field_validator, StrictInt, StrictStr
from typing_extensions import Self

from gretel_client.rest_v1.models.config_tenant_key import ConfigTenantKey
from gretel_client.rest_v1.models.llm_config import LlmConfig


class ServerlessTenantConfig(BaseModel):
    """
    ServerlessTenantConfig
    """  # noqa: E501

    cell_id: StrictStr
    api_endpoint: StrictStr
    enabled_llms: Optional[List[LlmConfig]] = None
    tier: Optional[StrictInt] = None
    revision: Optional[StrictStr] = None
    tenant_type: Optional[StrictStr] = None
    branch: Optional[StrictStr] = None
    state: Optional[StrictStr] = None
    disk_size_gb: Optional[StrictInt] = Field(
        default=None, description="size of disks to provision for the tenant"
    )
    keys: Optional[List[ConfigTenantKey]] = Field(
        default=None, description="keys required for CMK, provided by the customer"
    )
    __properties: ClassVar[List[str]] = [
        "cell_id",
        "api_endpoint",
        "enabled_llms",
        "tier",
        "revision",
        "tenant_type",
        "branch",
        "state",
        "disk_size_gb",
        "keys",
    ]

    @field_validator("tenant_type")
    def tenant_type_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(["UNKNOWN", "CUSTOMER", "DEMO", "TESTING", "SANDBOX"]):
            raise ValueError(
                "must be one of enum values ('UNKNOWN', 'CUSTOMER', 'DEMO', 'TESTING', 'SANDBOX')"
            )
        return value

    @field_validator("state")
    def state_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(["TENANT_STATE_UNKNOWN", "ACTIVE", "SUSPENDED"]):
            raise ValueError(
                "must be one of enum values ('TENANT_STATE_UNKNOWN', 'ACTIVE', 'SUSPENDED')"
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
        """Create an instance of ServerlessTenantConfig from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of each item in enabled_llms (list)
        _items = []
        if self.enabled_llms:
            for _item in self.enabled_llms:
                if _item:
                    _items.append(_item.to_dict())
            _dict["enabled_llms"] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in keys (list)
        _items = []
        if self.keys:
            for _item in self.keys:
                if _item:
                    _items.append(_item.to_dict())
            _dict["keys"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of ServerlessTenantConfig from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "cell_id": obj.get("cell_id"),
                "api_endpoint": obj.get("api_endpoint"),
                "enabled_llms": (
                    [LlmConfig.from_dict(_item) for _item in obj["enabled_llms"]]
                    if obj.get("enabled_llms") is not None
                    else None
                ),
                "tier": obj.get("tier"),
                "revision": obj.get("revision"),
                "tenant_type": obj.get("tenant_type"),
                "branch": obj.get("branch"),
                "state": obj.get("state"),
                "disk_size_gb": obj.get("disk_size_gb"),
                "keys": (
                    [ConfigTenantKey.from_dict(_item) for _item in obj["keys"]]
                    if obj.get("keys") is not None
                    else None
                ),
            }
        )
        return _obj
