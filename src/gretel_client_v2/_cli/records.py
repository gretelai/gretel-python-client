import click

from gretel_client_v2._cli.common import (
    SessionContext,
    model_option,
    project_option,
    pass_session,
    runner_option,
)


@click.group(help="Commands for working with records and running models.")
def records():
    ...


def model_path_option(fn):
    return click.option(
        "--model-path",
        metavar="PATH",
        help="Specify a remote path to the model.",
    )(fn)


def input_data_option(fn):
    return click.option(
        "--in-data",
        metavar="PATH",
        help="Specify model input data.",
    )(fn)


def output_data_option(fn):
    return click.option(
        "--output",
        metavar="DIR",
        help="Specify model output directory.",
    )(fn)


@records.command(help="Generate synthetic records from a model.")
@project_option
@runner_option
@model_option
@model_path_option
@pass_session
def generate(sc: SessionContext, project: str, runner: str, model: str):
    sc.log.info(f"got model {sc.model.name}")


@records.command(help="Transform records via pipelines.")
@project_option
@runner_option
@model_option
@model_path_option
@input_data_option
@output_data_option
@pass_session
def transform(
    sc: SessionContext, project: str, model_path: str, in_data: str, output: str
):
    ...


@records.command(help="Classify records using NER models.")
@project_option
@runner_option
@model_option
@model_path_option
@pass_session
def classify(sc: SessionContext, project: str):
    ...
