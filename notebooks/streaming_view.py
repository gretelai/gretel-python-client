# Set up visualization
import time

from IPython import display
import pandas as pd

from gretel_client.transformers import DataTransformPipeline


def display_df(
    df: pd.DataFrame, sleep: float, clear: bool, title: str, title_color: str
):
    style = df.style.apply(highlight_tags, cols=["tags"])
    if title:
        style = style.set_caption(title).set_table_styles(
            [
                {
                    "selector": "caption",
                    "props": [("color", title_color), ("font-size", "14px")],
                }
            ]
        )
    if clear:
        display.clear_output(wait=True)
    display.display(style)
    time.sleep(sleep)


def highlight_tags(s, cols) -> list:
    """ Style the discovered entities

    Params:
    s : series
    cols : list, list of columns to style
    """
    color_map = ["#47E0B3", "#F98043", "#50D8F1", "#C18DFC"]
    return [
        "background-color: {}".format(color_map[ord(str(x)[-1]) % len(color_map) - 1])
        if len(str(x)) > 0 and s.name in cols
        else ""
        for x in s
    ]


def stream_table_view(
    data: dict,
    xf: DataTransformPipeline = None,
    sleep: float = 0.0,
    title: str = None,
    title_color: str = "black",
    clear: bool = False,
):
    """
    Stream a table view into a Jupyter cell
    """
    if xf:
        transformed = xf.transform_record(data)
        df = pd.DataFrame.from_dict(
            transformed["record"], orient="index", columns=["field"]
        )
        df["tags"] = ""
        for field, value in transformed["record"].items():
            if field in data["record"].keys():
                if value != data["record"][field]:
                    df.at[field, "tags"] = "Transformed"
                else:
                    field_data = data["metadata"]["fields"].get(
                        str(field), {"ner": {"labels": []}}
                    )
                    labels = ", ".join(
                        [x["label"] for x in field_data["ner"]["labels"]]
                    )
                    df.at[field, "tags"] = labels
            else:
                df.at[field, "tags"] = "Transformed"
    else:
        # Gretel format record +
        df = pd.DataFrame.from_dict(data["record"], orient="index", columns=["field"])
        df["tags"] = ""
        for field in list(df.index):
            field_data = data["metadata"]["fields"].get(
                str(field), {"ner": {"labels": []}}
            )
            labels = ", ".join([x["label"] for x in field_data["ner"]["labels"]])
            df.at[field, "tags"] = labels
    display_df(df, sleep, clear, title, title_color)


def entries_list_view(
    d: dict,
    sleep: float = 0.0,
    clear: bool = False,
    title: str = None,
    title_color: str = "black",
):
    df = pd.DataFrame.from_dict(d, orient="index", columns=["entries"])
    display_df(df, sleep, clear, title, title_color)
