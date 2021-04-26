import click


@click.group()
def projects():
    click.echo("projects subcommand")


@projects.command()
def create():
    click.echo("creating a new project")


@projects.command()
def set_default():
    click.echo("setting default project")
