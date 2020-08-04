"""
This module contains the base ``Transformer`` class that can be used
create an actual transformer instance. It essentially is used as a factory
that takes a specific config object as the only argument at construction time.

NOTE:
    The primary interface in this module is the ``Transformer`` class which
    acts as a factory. While you may init any ``Transformer`` directly and interact
    with it, generally the individual objects are inserted into a ``RecordTransformerPipeline``
    which is the primary / preferred interface.
"""
import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
from numbers import Number
from typing import Mapping, Optional, Tuple, Union, List, TYPE_CHECKING

if TYPE_CHECKING:
    from gretel_client.transformers.restore import (
        RestoreTransformerConfig,
        RestoreTransformer,
    )
else:
    RestoreTransformer = None
    RestoreTransformerConfig = None


class Score:
    """
    Standard entity score values to help define minimum_scores for transformers.
    """
    LOW = .2
    MED = .5
    HIGH = .8
    MAX = 1.0


@dataclass(frozen=True)
class TransformerConfig(ABC):
    """An abstract dataclass that all Transformer
    Configs will inherit from.

    Should not need to be used directly.

    Args:
        labels: List of entity types that this transformer will be applied to.
        minimum_score: Any entity must have at least this score for the transformer to be applied.
    """

    labels: List[str] = None
    minimum_score: Optional[float] = None

    def __getstate__(self):
        return dict(self.__dict__)

    def __setstate__(self, state):
        for slot, value in state.items():
            object.__setattr__(self, slot, value)


@dataclass
class FieldRef:
    """A container that can be used to indicate
    that the contained ``name`` is referencing
    the name of the field.

    This object can be used as input to the ``tweak``
    param for certain transformer configs.
    """

    field_name: Union[List[str], str]
    radix: int = 10
    value: Union[List[str], List[Number], str, Number] = None


class Transformer(ABC):
    """The base class for all transformers that can act on input data.

    This class should be used direclty to created sub-classes of itself that contain
    transformer-specific logic. The only input to the constructor of this
    class is a config object.

    Args:
        config: Configuration object, which inherits from ``TransformerConfig`` describing the
            transformer type and parameters. See the specific configuration docs for the
    """

    config_class: TransformerConfig = None
    """Class attr that specifies the associated Config class a Transformer
    will use. Does not need to be modified or used directly
    """

    entity_sort_criterion = "start"

    def __init__(self, config: TransformerConfig):
        self.transform_entity_func = None
        self.labels = frozenset(config.labels or [])
        self.minimum_score = config.minimum_score
        self.field_ref_dict = dict(
            [
                (item[0], item[1])
                for item in config.__dict__.items()
                if isinstance(item[1], FieldRef)
            ]
        )

    def _get_field_ref(self, ref: str) -> FieldRef:
        return self.field_ref_dict.get(ref)

    def transform_entities(
        self, value: Union[Number, str], meta: dict
    ) -> Tuple[Optional[str], dict]:
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
             Returns ``None`` if no transformation occurred. This could be because no label or value was provided
             or if the transformer does not apply to the provided label.
         """

        # Short-circuit: if there is no metadata, we don't have knowledge about any entities
        if not meta:
            return value, {}

        self.transform_entity_func = self.transform_entity
        # Sort NER entities according to the criterion.
        # Drop entities if their score is below the minimum when the minimum is defined.
        raw_entities = [elt for elt in meta.get("ner", {}).get("labels", [])
                        if self.minimum_score is None or
                        (elt.get("score") is not None and elt.get("score", 0.0) >= self.minimum_score)]
        entities = sorted(
            raw_entities,
            key=lambda lbl: lbl[Transformer.entity_sort_criterion],
        )

        return self._transform_entities_base(value, meta, entities)

    def _transform_entities_base(
        self, value: Union[Number, str], meta: dict, entities
    ) -> Tuple[Optional[str], dict]:
        # check if any entities should trigger a total field drop
        all_ents = frozenset(e["label"] for e in entities)

        if self.__class__.__name__ == "Drop":
            if not self.labels.isdisjoint(all_ents):
                return None, {}

        # if the value is a Number, we won't recurse along a string
        # to do the whole entity overlap game, we just submit the value
        # to the transformers

        if isinstance(value, Number):
            for ent_label in all_ents:  # unlikely this is > 1, but just incase
                transformed_value_entities = self.transform_entity_func(
                    ent_label, value
                )
                if transformed_value_entities is not None:
                    # NOTE(jm): take the first successful transform
                    # if we run into a situation where a number
                    # has multiple entities mapped to it
                    # we could go by score, etc.
                    return transformed_value_entities[1], copy.deepcopy(meta)
                else:
                    return str(value), copy.deepcopy(meta)

        transformed_value, transformed_entities = self._transform_recursive(
            value, entities, None
        )
        transformed_meta = copy.deepcopy(meta)
        transformed_meta["ner"]["labels"] = transformed_entities
        return transformed_value, transformed_meta

    def transform_entity(
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
            new_label, new_value = self._transform_entity(  # pylint: disable=assignment-from-no-return
                label, value
            )
            if new_value:
                return new_label, new_value
        return label, value

    @abstractmethod
    def _transform(self, value: Union[Number, str]) -> Union[Number, str]:
        """This method should be overloaded by subclasses as it implements the actual logic"""
        pass

    def _transform_entity(self, label: str, value: Union[Number, str]) -> Optional[Tuple[Optional[str], str]]:
        """This method can be overloaded by subclasses if the logic needs to return a label other than None"""
        return None, self._transform(value)

    def transform_field(
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
        return self._transform_field(field, value, field_meta)

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        """This method can be overloaded by subclasses if the logic needs to return a field name other than the original
        field name"""
        if value is None:
            return {field: value}
        if isinstance(value, str) and not value:
            return {field: value}
        return {field: self._transform(value)}

    def _transform_recursive(
        self,
        value: Union[Number, str],
        entities: List[dict],
        transformed_entities: Union[List[dict], None],
    ) -> Tuple[Union[Number, str], dict]:
        """
        Performs a recursive transformation.

        The recursion is performed by first finding the first entity in `entities` that is transformable.
        This function is then invoked recursively on the values left and right of the respective entity, passing only
        entities that are fully within the respective part to the recursive invocations.
        Additionally, offsets of entities passed to the recursive invocation on the right part are transformed such that
        they are relative to the start of the right part.

        :param value: the value of the field.
        :param entities: the entities in the field that should be transformed.
        :return: the new field value.
        """
        if transformed_entities is None:
            transformed_entities = []
        while entities:
            current_entity, *entities = entities
            current_entity = copy.deepcopy(current_entity)
            transform_result = self.transform_entity_func(
                current_entity["label"], current_entity["text"]
            )
            new_label, new_text = transform_result

            # Partition the entity list into a left and right part.
            # This may drop entities which are not fully contained in the left or right part, respectively, which is
            # okay.
            left_entities = [
                ent for ent in entities if ent["end"] <= current_entity["start"]
            ]
            right_entities = [
                {
                    **ent,
                    "start": ent["start"] - current_entity["end"],
                    "end": ent["end"] - current_entity["end"],
                }
                for ent in entities
                if ent["start"] >= current_entity["end"]
            ]

            left_transformed_value, left_transformed_entity = self._transform_recursive(
                value[: current_entity["start"]], left_entities, transformed_entities
            )
            if len(left_transformed_entity) > 0:
                raise Exception(
                    "_transform_recursive was called with an unsorted entity list."
                )

            (
                right_transformed_value,
                right_transformed_entity,
            ) = self._transform_recursive(
                value[current_entity["end"]:], right_entities, transformed_entities
            )

            # Stitch together the new field value.
            transformed_value = (
                left_transformed_value + new_text + right_transformed_value
            )

            right_offset = len(left_transformed_value) + len(new_text)
            for entity in right_transformed_entity:
                entity["start"] += right_offset
                entity["end"] += right_offset

            current_entity["end"] = len(new_text) + current_entity["start"]
            current_entity["text"] = new_text
            current_entity["label"] = new_label
            transformed_entities.insert(0, current_entity)
            return transformed_value, transformed_entities

        # No further entities to transform, return the field value as-is.
        return value, transformed_entities


def _build_config_map(cls, _map=None):
    if _map is None:
        _map = []
    for xform_class in cls.__subclasses__():
        if not xform_class.__subclasses__():
            _map.append((xform_class.config_class, xform_class))
        else:
            _build_config_map(xform_class, _map)
    return _map


def factory(
    config: Union[TransformerConfig, RestoreTransformerConfig]
) -> Union[Transformer, RestoreTransformer]:
    """Factory that returns a ``Transformer`` subclass instance.

    Given a specific config, we will enumerate all of the mappings
    of a config class to the actual transformer class. This allows
    for the relationship between a config and the transformer class to
    be derived automatically.

    We need to do this on each instantiation as there is no guarantee that
    every config class has been imported to the global module table.  Since
    this only happens when building the transform pipeline, it does not have
    any material impact on the effeciency of doing actual transforms.

    Args:
        config: A ``TransformerConfig`` subclass instance

    Returns:
        A ``Transformer`` subclass instance
    """
    _map = dict(_build_config_map(Transformer))
    try:
        _class = _map[config.__class__]
    except KeyError:
        raise ValueError("config is not a TransformerConfig subclass")

    return _class(config)
