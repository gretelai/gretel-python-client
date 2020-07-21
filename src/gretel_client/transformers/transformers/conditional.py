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
    """Run a transform based on checking the value of another field. You may use a regex
    to check that the value of a field matches or not. If the regex matches, then the
    ``true_xform`` transform will run, otherwise, the ``false_xform`` will run.

    If either the true or false transform configurations are ``None`` then the record
    will not be transformed depending on the matched case.

    In this example we'll redact with X if the conditional field contains "foo" and
    Y if it does not::

        xf = ConditionalConfig(
            conditional_value=FieldRef("check_me"),
            regex=r"foo",
            true_xform=RedactWithChar(char="X"),
            false_xform=RedactWithChar(char="Y")
        )

    Args:
        conditional_value: A ``FieldRef`` that defines what field to check for the regex match
        regex: A regex string that is used to check the value of the ``conditional_value`` field
        true_xform: A transform config that will be run if the regex matches, if `None`, no transform is run
        false_xform: A transform config that will run if the regex does not match, if `None`, no transform is run
    """
    conditional_value: FieldRef = None
    regex: Union[str, Pattern] = None
    true_xform: TransformerConfig = None
    false_xform: TransformerConfig = None


class Conditional(RestoreTransformer):
    config_class = ConditionalConfig
    true_xform: Transformer = None
    false_xform: Transformer = None
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
        if config.false_xform is not None:
            self.false_xform = factory(config.false_xform)

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
                if isinstance(self.true_xform, RestoreTransformer):
                    return self.true_xform.restore_entity(label, value)
                else:
                    return self.true_xform.transform_entity(label, value)
            else:
                return label, value
        else:
            if self.false_xform:
                if isinstance(self.false_xform, RestoreTransformer):
                    return self.false_xform.restore_entity(label, value)
                else:
                    return self.false_xform.transform_entity(label, value)
            else:
                return label, value

    def _restore_field(self, field: str, value: Union[Number, str], field_meta):
        if self.regex.match(self._get_field_ref('conditional_value').value):
            if self.true_xform:
                if isinstance(self.true_xform, RestoreTransformer):
                    return self.true_xform.restore_field(field, value, field_meta)
                else:
                    return self.true_xform.transform_field(field, value, field_meta)
            else:
                return {field: value}
        else:
            if self.false_xform:
                if isinstance(self.false_xform, RestoreTransformer):
                    return self.false_xform.restore_field(field, value, field_meta)
                else:
                    return self.false_xform.transform_field(field, value, field_meta)
            else:
                return {field: value}
