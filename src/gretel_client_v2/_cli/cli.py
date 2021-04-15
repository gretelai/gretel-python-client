import os

import click

from gretel_client_v2.configuration import Configuration
from gretel_client_v2.api_client import ApiClient
from gretel_client_v2.api.default_api import DefaultApi

from gretel_cli.configure import configure
from gretel_cli.models import models
from gretel_cli.projects import projects
from gretel_cli.records import records


def api_client() -> DefaultApi:
    configuration = Configuration(host="https://api-dev.gretel.cloud")
    configuration.api_key['Authorization'] = os.getenv("GRETEL_API_KEY")
    api_client = ApiClient(configuration)
    return DefaultApi(api_client)


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))


cli.add_command(configure)
cli.add_command(models)
cli.add_command(records)
cli.add_command(projects)


if __name__ == '__main__':
    cli()
