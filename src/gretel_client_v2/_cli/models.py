import click


@click.group()
def models():
    click.echo("models subcommand")


@click.command()
def create():
    click.echo("creating a model")


@click.command()
def get():
    click.echo("getting a model")


models.add_command(create)
models.add_command(get)
