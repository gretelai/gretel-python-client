import re
from dataclasses import dataclass
from numbers import Number
from typing import Optional, Tuple, Union

from gretel_client.transformers.restore import RestoreTransformer, RestoreTransformerConfig
from gretel_client.transformers.base import Transformer, TransformerConfig, FieldRef, factory

try:
    from re import Pattern
except ImportError:
    from typing import Pattern


@dataclass(frozen=True)
class ConditionalConfig(RestoreTransformerConfig):
    conditional_value: FieldRef = None
    regex: Union[str, Pattern] = None
    true_xform: TransformerConfig = None
    false_xform: TransformerConfig = None


class Conditional(RestoreTransformer):
    """
    Conditional transformer give a value compared to a regex, it calls one or another transformer based on the result.
    The true or false case transformer parameters default to pass the data through if not passed in.
    """
    config_class = ConditionalConfig
    true_xform: Union[RestoreTransformer, Transformer] = None
    false_xform: Union[RestoreTransformer, Transformer] = None
    regex: Pattern

    def __init__(self, config: ConditionalConfig):
        super().__init__(config=config)
        self.conditional_value = config.conditional_value
        if isinstance(config.regex, Pattern):
            self.regex = config.regex
        else:
            self.regex = re.compile(config.regex)
        if config.true_xform is not None:
            self.true_xform = factory(config.true_xform)
            if isinstance(self.true_xform, RestoreTransformer):
                self._true_xform_restore_field = self.true_xform.restore_field
                self._true_xform_restore_entity = self.true_xform.restore_entity
            else:
                self._true_xform_restore_field = self.true_xform.transform_field
                self._true_xform_restore_entity = self.true_xform.transform_entity

        if config.false_xform is not None:
            self.false_xform = factory(config.false_xform)
            if isinstance(self.false_xform, RestoreTransformer):
                self._false_xform_restore_field = self.false_xform.restore_field
                self._false_xform_restore_entity = self.false_xform.restore_entity
            else:
                self._false_xform_restore_field = self.false_xform.transform_field
                self._false_xform_restore_entity = self.false_xform.transform_entity

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        if self.regex.match(self._get_field_ref('conditional_value').value):
            if self.true_xform:
                return self.true_xform.transform_entity(label, value)
            else:
                return label, value
        else:
            if self.false_xform:
                return self.false_xform.transform_entity(label, value)
            else:
                return label, value

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        if self.regex.match(self._get_field_ref('conditional_value').value):
            if self.true_xform:
                return self.true_xform.transform_field(field, value, field_meta)
            else:
                return {field: value}
        else:
            if self.false_xform:
                return self.false_xform.transform_field(field, value, field_meta)
            else:
                return {field: value}

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        if self.regex.match(self._get_field_ref('conditional_value').value):
            if self.true_xform:
                return self._true_xform_restore_entity(label, value)
            else:
                return label, value
        else:
            if self.false_xform:
                return self._false_xform_restore_entity(label, value)
            else:
                return label, value

    def _restore_field(self, field: str, value: Union[Number, str], field_meta):
        if self.regex.match(self._get_field_ref('conditional_value').value):
            if self.true_xform:
                return self._true_xform_restore_field(field, value, field_meta)
            else:
                return {field: value}
        else:
            if self.false_xform:
                return self._false_xform_restore_field(field, value, field_meta)
            else:
                return {field: value}

    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        raise ValueError(
            "_transform method was called even though the Drop transformer does not implement this!")

    def _restore(self, value: Union[Number, str]) -> Union[Number, str]:
        raise ValueError(
            "_restore method was called even though the Drop transformer does not implement this!")

