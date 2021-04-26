import click

from gretel_client_v2._cli.configure import configure
from gretel_client_v2._cli.models import models
from gretel_client_v2._cli.projects import projects
from gretel_client_v2._cli.records import records


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
