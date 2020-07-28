"""
Module for misc demo helpers
"""
from typing import List
import logging
import random
import pprint
import difflib

from gretel_client.projects import Project
from gretel_client.transformers import (
    DropConfig,
    FakeConstantConfig,
    RedactWithCharConfig,
    RedactWithLabelConfig,
    RedactWithStringConfig,
    StringMask,
    DataPath,
    bucket_creation_params_to_list,
    BucketCreationParams,
    BucketConfig,
    DateShiftConfig
)
from gretel_client.transformers.fakers import FAKER_MAP

try:
    from gretel_client.transformers import FpeStringConfig
    from gretel_client.transformers import DateShiftConfig
except ImportError:
    logging.warn(f'Cannot load Fpe and DateShift libraries.'
                 f'Gretel Format Preserving Encryption module is not installed.')

pp = pprint.PrettyPrinter(indent=2)


SEED = 6251
SECRET = "2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94"


def show_record_diff(original: dict, transformed: dict):
    old = '\n'.join([f'{key}:{value}' for key, value in sorted(original.items())])
    new = '\n'.join([f'{key}:{value}' for key, value in sorted(transformed.items())])

    for diffs in difflib.unified_diff(old.splitlines(), new.splitlines(), fromfile="original", tofile="transformed"):
        print(diffs)


class RandomTransformerPipeline:

    entity_types: List[str]
    id_entities = List[str]
    location_entities = List[str]
    time_entities = List[str]

    person_name_fields = List[str]

    data_paths = List[DataPath]

    def __init__(self, project: Project):
        self.project = project
        self.data_paths = []

    def detect_entities(self) -> List[str]:
        """Given a Gretel project, get a list of entitites
        that we will use to generate a random anonymization / redaction
        routine around
        """
        self.entity_types = [d["entity"] for d in self.project.entities]

        # Let's look for some identifiers... We will filter these examples against what we actually found.
        id_entities = ["person_name", "email_address", "ip_address", "uuid"]
        self.id_entities = [e for e in id_entities if e in self.entity_types]

        self.person_name_fields = [
            d["field"] for d in self.project.get_field_details(entity="person_name")
        ]

        # And some places, both strings and numbers if we can...
        location_entities = ["city", "us_zip_code", "latitude", "longitude"]
        self.location_entities = [
            e for e in location_entities if e in self.entity_types
        ]

        # Everyone loves working with dates.
        time_entities = ["date", "datetime"]
        self.time_entities = [e for e in time_entities if e in self.entity_types]

        pp.pprint(f"Detected entity types: {self.entity_types}")

    def build_anonymizing_transforms(self):
        for entity in self.id_entities:
            # Get all the project fields tagged as this entity type
            entity_fields = [
                d["field"] for d in self.project.get_field_details(entity=entity)
            ]
            for field in entity_fields:
                dice_roll = random.randint(1, 6)
                xf = []
                if dice_roll == 1:
                    print(f"Dropping field {field}")
                    xf = [DropConfig()]
                if dice_roll == 2:
                    print(f"Faking field {field}")
                    xf = [
                        FakeConstantConfig(
                            seed=SEED, fake_method=FAKER_MAP.get(entity, "name")
                        )
                    ]
                if dice_roll == 3:
                    print(f"Encrypting field {field}")
                    # radix 62 will encrypt alphanumeric but no special characters
                    xf = [FpeStringConfig(secret=SECRET, radix=62)]
                if dice_roll == 4:
                    print(f"Character redacting field {field}")
                    # Use a fancier mask for emails
                    if entity == "email_address":
                        xf = [
                            RedactWithCharConfig(
                                char="X",
                                mask=[
                                    StringMask(start_pos=3, mask_until="@"),
                                    StringMask(
                                        mask_after="@", mask_until=".", greedy=True
                                    ),
                                ],
                            )
                        ]
                    else:
                        xf = [RedactWithCharConfig("#", mask=[StringMask(start_pos=3)])]
                if dice_roll == 5:
                    print(f"Label redacting field {field}")
                    xf = [RedactWithLabelConfig(labels=[entity])]
                if dice_roll == 6:
                    print(f"String redacting field {field}")
                    xf = [RedactWithStringConfig(string="CLASSIFIED")]

                self.data_paths.append(DataPath(input=field, xforms=xf))

    def build_location_transforms(self):
        for entity in self.location_entities:
            # Get all the project fields tagged as this entity type
            entity_fields = [
                d["field"] for d in self.project.get_field_details(entity=entity)
            ]
            for field in entity_fields:
                xf = []
                if entity in ["city", "us_zip_code"]:
                    print(f"Faking field {field}")
                    xf = [FakeConstantConfig(seed=SEED, fake_method=FAKER_MAP[entity])]
                else:
                    print(f"Rounding field {field}")
                    min_max_width_tuple = BucketCreationParams(-180.0, 180.0, 0.1)
                    buckets = bucket_creation_params_to_list(min_max_width_tuple)
                    xf = [BucketConfig(buckets=buckets)]

                self.data_paths.append(DataPath(input=field, xforms=xf))

    def build_date_shift_transforms(self):
        for entity in self.time_entities:
            # Get all the project fields tagged as this entity type
            entity_fields = [
                d["field"] for d in self.project.get_field_details(entity=entity)
            ]
            for field in entity_fields:
                print(f"Date shifting field {field}")
                xf = [
                    DateShiftConfig(
                        secret=SECRET, lower_range_days=-10, upper_range_days=25
                    )
                ]
                self.data_paths.append(DataPath(input=field, xforms=xf))
