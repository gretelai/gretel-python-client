import click


@click.group()
def models():
    click.echo("models subcommand")


@models.command()
def create():
    click.echo("creating a model")


@models.command()
def get():
    click.echo("getting a model")
