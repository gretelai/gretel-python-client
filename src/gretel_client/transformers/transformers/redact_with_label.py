from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union
from gretel_client.transformers.base import TransformerConfig, Transformer


@dataclass(frozen=True)
class RedactWithLabelConfig(TransformerConfig):
    """Redact a string with the name of an entity label that represents that data.  This transformer
    is best used with already labeled data from the Gretel API. If you created a version of this
    transformer to redact an email address, then each email address would be replaced with
    "EMAIL_ADDRESS".

    Example::

        xf = RedactWithLabelConfig(labels=["email_address"])

    If this is used as a field transformer, the transformer will attempt to find any entity label metadata
    for that field. If found, the first label observed will be the string used for the redaction. The entire field
    contents will be replaced with the redaction.
    """
    pass


class RedactWithLabel(Transformer):
    config_class = RedactWithLabelConfig

    def _transform_field(self, field_name: str, field_value: Union[Number, str], field_meta):
        if field_meta is not None:
            try:
                label = field_meta['ner']['labels'][0]['label']
            except KeyError:
                return None
            else:
                return {field_name: label.upper()}
        else:
            return None

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        return None, label.upper()

    def _transform(self, value) -> Union[Number, str]:
        raise ValueError(
            "_transform method was called even though _transform_field and _transform_entity are overloaded!")
