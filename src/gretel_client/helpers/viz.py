from dataclasses import dataclass
from typing import Type, List
from copy import deepcopy

import pandas as pd
from IPython.display import display, Markdown
import ipywidgets as widgets
from ipywidgets import (
    HBox,
    interactive_output,
    VBox,
)

from gretel_client.projects import Project

MD1 = """
## Field Selection

The widgets below provide an initial start on how you can filter out interesting fields from the
Gretel Metastore.  These interesting fields can be used to build transformations and synthetic data.

As you interact with the widgets the field table will adjust automatically. Additionally, the second
table will show you a preview of your records based on these adjustments.

### Quantative

The two sliders below allow you to explore two metrics that are tracked for all fields discovered by Gretel.

- How unique is the value of a field? The first slider lets you adjust the percent of uniqueness in a field.

- How often is this field missing or null? The second slider lets you adjust what percent of a
field does not have any data.


### Qualatative

Below the sliders, are checkboxes that represent entities which have been discovered in your data. Selecting
certain entities will further filter the visible fields. You can see all of the entities for a given field
under the `entities` column. Additional detail for each entity per-field can be explored via the REST API
or gretel-client.


### Additional Exploration

To further explore the field metadata from the Gretel Metastore please visit your project in our Console or take a
look at the full field metadata by using our gretel-client or REST API.

***
"""


@dataclass
class FieldFilter:
    original_df: Type[pd.DataFrame] = None
    current_df: Type[pd.DataFrame] = None
    record_df: Type[pd.DataFrame] = None
    record_fields: List[str] = None

    def load_records(self, records: List[dict]):
        self.record_df = pd.DataFrame(records)
        self.record_fields = list(self.record_df)

    @property
    def fields(self):
        return list(self.current_df["field"])

    def export_fields(self):
        return deepcopy(self.fields)


def start_field_filtering(project: Project, field_filter: FieldFilter):
    field_meta = project.get_field_details()
    records = project.sample(n=20)
    _start_field_filtering(field_meta, records, field_filter)


def _build_main_df(field_meta: dict) -> pd.DataFrame:
    df = pd.DataFrame(field_meta)
    df = df[
        [
            "field",
            "pct_relative_unique",
            "pct_missing",
            "approx_cardinality",
            "count",
            "entities",
        ]
    ]
    df["entities"] = df["entities"].apply(lambda arr: [ent["label"] for ent in arr])
    return df


def _build_entity_checkboxes(df: pd.DataFrame) -> dict:
    all_ents = []
    for ents in df["entities"]:
        all_ents.extend(ents)
    out = {}
    for e in all_ents:
        out["entity_" + e] = widgets.Checkbox(
            value=False,
            description=e,
            disabled=False,
            indent=False,
            style=dict(description_width="initial"),
        )
    return out


def _start_field_filtering(field_meta: dict, records: list, field_filter: FieldFilter):
    receiver = field_filter
    receiver.original_df = _build_main_df(field_meta)
    receiver.load_records(records)

    def modify_view(**kwargs):
        df = receiver.original_df
        filtered = df[
            (df["pct_relative_unique"] < kwargs["max_unique"])
            & (df["pct_missing"] < kwargs["max_missing"])
        ]

        filter_ents = set()
        for k, v in kwargs.items():
            if k.startswith("entity_") and v:
                filter_ents.add(k.split("entity_")[-1])

        if filter_ents:
            filtered = filtered[
                filtered.apply(
                    lambda row: len(set(row["entities"]) & filter_ents) > 0,
                    axis=1
                )
            ]
        receiver.current_df = filtered
        display(receiver.current_df)

        # First we make sure that all the fields are part of the
        # sample records, for the ones that are not, we display
        # a warning
        filter_fields = []
        missing_fields = []
        for field in receiver.fields:
            if field not in receiver.record_fields:
                missing_fields.append(field)
            else:
                filter_fields.append(field)

        display(Markdown('## Sample Training Data'))
        display(receiver.record_df[filter_fields])

    unique_slider = widgets.FloatSlider(
        min=0.0,
        max=100.0,
        step=1,
        value=80,
        description="Max Unique %",
        style=dict(description_width="initial"),
    )
    missing_slider = widgets.FloatSlider(
        min=0.0,
        max=100.0,
        step=1,
        value=25,
        description="Max Missing %",
        style=dict(description_width="initial"),
    )
    checkboxes = _build_entity_checkboxes(receiver.original_df)

    sliders = HBox([unique_slider, missing_slider])
    boxes = HBox(list(checkboxes.values()))
    ui = VBox([sliders, boxes])
    widget_map = {"max_unique": unique_slider, "max_missing": missing_slider}
    widget_map.update(checkboxes)
    out = interactive_output(modify_view, widget_map)
    display(Markdown(MD1))
    display(ui, out)
