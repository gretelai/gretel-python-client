import click


@click.command()
def records(ctx):
    click.echo("records subcommand")
