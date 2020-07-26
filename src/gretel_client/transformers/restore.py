"""
This module contains the base ``Transformer`` class that can be used
create an actual transformer instance. It essentially is used as a factory
that takes a specific config object as the only argument at construction time.

NOTE:
    The primary interface in this module is the ``Transformer`` class which
    acts as a factory. While you may init any ``Transformer`` directly and interact
    with it, generally the individual objects are inserted into a ``RecordTransformerPipeline``
    which is the primary interface.
"""
from abc import abstractmethod
from dataclasses import dataclass
from numbers import Number
from typing import Mapping, Optional, Tuple, Union
import logging

from gretel_client.transformers.base import TransformerConfig, Transformer

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class RestoreTransformerConfig(TransformerConfig):
    """An abstract dataclass that all Transformer
    Configs will inherit from.

    Should not need to be used directly.
    """
    pass


class RestoreTransformer(Transformer):
    """The base class for all transformers that can act on input data.

    This class should be used direclty to created sub-classes of itself that contain
    transformer-specific logic. The only input to the constructor of this
    class is a config object.

    Returns:
        An instance of ``Transformer``
    """

    config_class: RestoreTransformerConfig = None
    """Class attr that specifies the associated Config class a Transformer
    will use. Does not need to be modified or used directly
    """

    entity_sort_criterion = "start"
    entity_sort_reversed = False

    def restore_entities(
        self, value: Union[Number, str], meta: dict
    ) -> Tuple[str, dict]:
        """
         Transforms all, labeled entities that occur within a field. This is the primary entrypoint
         and should not be overloaded. We maintain this as the single entrypoint so that we can check if the
         provided labels are the ones that we should act on, and if so, pass it to the sub-class specific handler

         Args:
             value: the entity value, such as 'john.doe@gmail.com'.
             meta: the metadata associated with the value.
         Returns:
             tuple: (transformed_value, transformed_meta) if a transformation occurred

         NOTE:
             Returns ``None`` if no transformation occurred. This could be, because no label or value was provided
             or if the transformer does not apply to the provided label.
         """

        # Short-circuit: if there is no metadata, we don't have knowledge about any entities
        if not meta:
            return value, {}
        self.transform_entity_func = self.restore_entity
        # Sort NER entities according to the criterion.
        entities = sorted(
            meta.get("ner", {}).get("labels", []),
            key=lambda lbl: lbl[Transformer.entity_sort_criterion],
            reverse=RestoreTransformer.entity_sort_reversed,
        )
        return self._transform_entities_base(value, meta, entities)

    def restore_entity(
        self, label: str, value: Union[Number, str]
    ) -> Optional[Tuple[str, str]]:
        """
        Transforms a single, labeled entity that occurs within a field. This is the primary entrypoint
        and should not be overloaded. We maintain this as the single entrypoint so that we can check if the
        provided label is one that we should act on, and if so, pass it to the sub-class specific handler

        Args:
            label: the entity label, such as "email_address".
            value: the entity value, such as 'john.doe@gmail.com'.

        Returns:
            tuple: (label, value) if a transformation occurred

        NOTE:
            Returns ``None`` if no transformation occurred. This could becaue no label or value was provied
            or if the transformer does not apply to the provided label.
        """
        if label and label in self.labels:
            (  # pylint: disable=assignment-from-no-return
                new_label,
                new_value,
            ) = self._restore_entity(
                label, value
            )
            if new_value:
                return new_label, new_value
        return label, value

    def _restore_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        """This method can be overloaded by subclasses if the logic needs to return a label other than label"""
        return label, self._restore(value)

    def restore_field(
        self, field: str, value: Union[Number, str], field_meta: Optional[dict]
    ) -> Mapping[str, str]:
        """
        Transforms a field within a record. The result of the transform can be multiple fields (including None),
        represented as a dict mapping each field name to its value.

        Args:
            field: the name of the field to be transformed.
            value: the value of the field to be transformed.
            field_meta: the metadata of the field to be transformed (may be None).

        Returns:
            dict: with all transformed fields.
        """
        try:
            return self._restore_field(field, value, field_meta)
        except Exception as err:
            logger.warning(f"Could not restore {field}:{value}. Error: {str(err)}")
            return {field: value}

    def _restore_field(self, field, value: Union[Number, str], field_meta):
        return {field: self._restore(value)}

    @abstractmethod
    def _restore(self, value: Union[Number, str]) -> Union[Number, str]:
        pass
