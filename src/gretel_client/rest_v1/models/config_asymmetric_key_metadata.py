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

from typing import Optional

from pydantic import BaseModel, Field, StrictStr, validator


class ConfigAsymmetricKeyMetadata(BaseModel):
    """
    ConfigAsymmetricKeyMetadata
    """

    key_id: Optional[StrictStr] = Field(
        None,
        description="A string to allow identifying the key used for decryption. This may reference a resource within a cloud provider; however, clients may treat this as a fully opaque value.",
    )
    algorithm: Optional[StrictStr] = Field(
        None, description="The asymmetric decryption algorithm to use with this key."
    )
    public_key_pem: Optional[StrictStr] = Field(
        None, description="PEM-encoded public key."
    )
    __properties = ["key_id", "algorithm", "public_key_pem"]

    @validator("algorithm")
    def algorithm_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ("UNKNOWN_ALGORITHM", "RSA_4096_OAEP_SHA256"):
            raise ValueError(
                "must be one of enum values ('UNKNOWN_ALGORITHM', 'RSA_4096_OAEP_SHA256')"
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
    def from_json(cls, json_str: str) -> ConfigAsymmetricKeyMetadata:
        """Create an instance of ConfigAsymmetricKeyMetadata from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self):
        """Returns the dictionary representation of the model using alias"""
        _dict = self.dict(by_alias=True, exclude={}, exclude_none=True)
        return _dict

    @classmethod
    def from_dict(cls, obj: dict) -> ConfigAsymmetricKeyMetadata:
        """Create an instance of ConfigAsymmetricKeyMetadata from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return ConfigAsymmetricKeyMetadata.parse_obj(obj)

        _obj = ConfigAsymmetricKeyMetadata.parse_obj(
            {
                "key_id": obj.get("key_id"),
                "algorithm": obj.get("algorithm"),
                "public_key_pem": obj.get("public_key_pem"),
            }
        )
        return _obj
