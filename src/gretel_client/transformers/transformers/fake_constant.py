from dataclasses import dataclass
from numbers import Number
from typing import Tuple, Optional, Union, List

from gretel_client.transformers.base import TransformerConfig, Transformer
from gretel_client.transformers.fakers import Fakers

# this maps Gretel Entity labels => Faker methods


FAKER_MAP = {
    "aba_routing_number": None,
    "age": None,
    "city": "city",
    "credit_card_number": "credit_card_number",
    "date": "date_between",
    "datetime": "date_time",
    "date_of_birth": "date_between",
    "domain_name": "domain_name",
    "email_address": "email",
    "ethnic_group": None,
    "first_name": "first_name",
    "gcp_credentials": None,
    "gender": None,
    "hostname": "hostname",
    "iban_code": "iban",
    "icd9_code": None,
    "icd10_code": None,
    "imei_hardware_id": None,
    "imsi_subscriber_id": None,
    "ip_address": "ipv4_public",
    "last_name": "last_name",
    "latitude": "latitude",
    "transform_latlon_1km": None,
    "transform_latitude_1km": None,
    "location": None,
    "longitude": "longitude",
    "transform_longitude_1km": None,
    "mac_address": "mac_address",
    "passport": None,
    "person_name": "name",
    "norp_group": None,
    "phone_number": "phone_number",
    "phone_number_namer": "phone_number",
    "organization_name": "company",
    "swift_code": None,
    "time": "time",
    "url": "url",
    "us_social_security_number": "ssn",
    "us_state": None,
    "us_zip_code": "postcode",
    "gpe": None,
    "user_id": None,
}
"""A mapping of Gretel Entities to Faker methods that are used to generate
fake values.  The keys of this mapping should be used as the ``fake_method``
param when creating a configuration.  If the value is ``None`` then that entity
is currently not suported for fake generation. """


VALID_LABELS = [key for key, value in FAKER_MAP.items() if value]


@dataclass(frozen=True)
class FakeConstantConfig(TransformerConfig):
    """Create a fake value of a certain entity / type given an input value.

    Args:
        seed: A required starting seed for the underlying fake generator. Use the same seed in order
            to get the same fake values for a given input.
        fake_method: A string of what kind of fake entity to create. One of the keys from the ``FAKER_MAP`` mapping.
        locales: A list of locales to use for generating fake values
        locale_seed: An optional seed to use for init'ing the order of locales to be used for creating fake values
    """
    seed: int = None
    fake_method: str = None
    locales: List[str] = None
    locale_seed: Union[int, None] = 0


class FakeConstant(Transformer):
    """
    FakeConstant transformer replaces the value with a new fake value.
    """

    config_class = FakeConstantConfig

    def __init__(self, config: FakeConstantConfig):
        super().__init__(config)
        self.faker = Fakers(
            seed=config.seed, locales=config.locales, locale_seed=config.locale_seed
        )
        self.fake_method = config.fake_method
        if self.labels:
            if not all(elem in VALID_LABELS for elem in self.labels):
                raise ValueError(
                    f"Labels list has to be one or more of {VALID_LABELS} only!"
                )
        elif not self.fake_method:
            raise ValueError(
                "No fake methods or labels which map to fake methods are specified!"
            )

    def _transform_field(self, field: str, value: Union[Number, str], field_meta):
        return {field: self.mutate(value, self.fake_method)}

    def _transform_entity(
        self, label: str, value: Union[Number, str]
    ) -> Optional[Tuple[Optional[str], str]]:
        fake_method = FAKER_MAP.get(label)
        if fake_method is None:
            return None
        new_value = self.mutate(value, fake_method)
        if isinstance(value, float):
            new_value = float(new_value)
        elif isinstance(value, str):
            new_value = str(new_value)
        return None, new_value

    def mutate(self, value, fake_method: str):
        return self.faker.constant_fake(value, fake_method)
