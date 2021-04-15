import click


@click.group()
def projects():
    click.echo("projects subcommand")


@click.command()
def create():
    click.echo("creating a new project")


@click.command()
def set_default():
    click.echo("setting default project")


projects.add_command(create)
projects.add_command(set_default)
