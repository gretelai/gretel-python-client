from dataclasses import dataclass
from typing import Type, List
from copy import deepcopy

import pandas as pd
from IPython.display import display, Markdown
import ipywidgets as widgets
from ipywidgets import (
    interactive,
    TwoByTwoLayout,
    Layout,
    HBox,
    interactive_output,
    VBox,
)

from gretel_client.projects import Project

MD1 = """
## Field Selection

Here you may choose which fields you include for synthetic data generation. Field selection happens sequentially
through the various options below.

First, you may narrow down fields based on how unique they are or how much missing data there is.

Second, if you are only interested in specific entities, you may select those.

The table below will automatically update to show you the fields that match your current selection.

The field list based on initial filtering is available via the `data.fields` property. To export the final
list you may call:

```python
target_fields = data.export_fields()
```

This will create a deep copy of the field list that you can manipulate freely.
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


def start_field_filtering(project: Project):
    pass


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


def filter_table_view(field_stats: list, fields: FieldFilter):
    """ Interactively view results of applying statistical filters to tables """
    receiver = fields.receiver
    df = pd.DataFrame(field_stats)
    df = df[
        ["field", "pct_relative_unique", "pct_missing", "approx_cardinality", "count"]
    ]
    df_original = df
    df_filtered = []
    selector = widgets.SelectMultiple(
        options=df_original["field"],
        value=df_filtered,
        layout=Layout(display="flex", flex_flow="column"),
    )

    def field_view(max_unique, max_missing):
        filtered = df[
            (df["pct_relative_unique"] < max_unique) & (df["pct_missing"] < max_missing)
        ]
        df_filtered = list(filtered["field"])
        selector.value = df_filtered
        receiver["df_filtered"] = filtered
        display(filtered)
        # return 'foo'
        # return filtered.sort_values(by=["pct_relative_unique"], ascending=False)

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

    ui = HBox([unique_slider, missing_slider])
    out = interactive_output(
        field_view, {"max_unique": unique_slider, "max_missing": missing_slider}
    )
    display(ui, out)


def filter_table_entities(field_ents: list):
    """ Interactively view results of applying statistical filters to tables """
    # receiver = fields.receiver
    df = pd.DataFrame(field_ents)
    df = df[
        ["field", "pct_relative_unique", "pct_missing", "approx_cardinality", "count"]
    ]
    df_original = df
    df_filtered = []
    selector = widgets.SelectMultiple(
        options=df_original["field"],
        value=df_filtered,
        layout=Layout(display="flex", flex_flow="column"),
    )

    def field_view(max_unique, max_missing):
        filtered = df[
            (df["pct_relative_unique"] < max_unique) & (df["pct_missing"] < max_missing)
        ]
        display(filtered)
        df_filtered = list(filtered["field"])
        selector.value = df_filtered
        receiver["df_filtered"] = filtered
        return filtered.sort_values(by=["pct_relative_unique"], ascending=False)

    unique_slider = widgets.FloatSlider(min=0.0, max=100.0, step=1, value=80)
    missing_slider = widgets.FloatSlider(min=0.0, max=100.0, step=1, value=25)
    # items = [selector]

    # def observer(widget, new_val):
    #    receiver[widget] = new_val

    # unique_slider.observe(partial(observer, "unique"), names="value")
    # missing_slider.observe(partial(observer, "missing"), names="value")
    #     selector_layout = Layout(width='auto')
    #     render = Box(children=items, layout=selector_layout)
    field_table = interactive(
        field_view, max_unique=unique_slider, max_missing=missing_slider
    )
    render = TwoByTwoLayout(top_left=field_table, top_right=selector,)
    display(field_table)


"""
def filter_table_view(field_meta: List[dict]):  # pragma: no cover

    df = pd.DataFrame(field_meta)
    df = df[
        ["field", "pct_relative_unique", "pct_missing", "approx_cardinality", "count"]
    ]

    def field_view(max_unique, max_missing):
        filtered = df[
            (df["pct_relative_unique"] < max_unique) & (df["pct_missing"] < max_missing)
        ]
        display.display(filtered)
        return filtered.sort_values(by=["pct_relative_unique"], ascending=False)

    unique_slider = widgets.FloatSlider(min=0.0, max=100.0, step=1, value=80)
    missing_slider = widgets.FloatSlider(min=0.0, max=100.0, step=1, value=25)

    display.display(
        interactive(field_view, max_unique=unique_slider, max_missing=missing_slider)
    )
"""
