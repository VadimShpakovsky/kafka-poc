import importlib

import click
import yaml

from utils.common import setup_logger

setup_logger()


@click.group()
def cli():
    pass


@click.group()
@click.option("--config", "-c", type=click.Path(exists=True), default="config.yaml")
@click.pass_context
def cli(ctx, config):
    with open(config) as f:
        ctx.obj = {"config": yaml.safe_load(f)}


@cli.command()
@click.option("--impl-name", required=True, help="Name of the implementation to use")
@click.pass_context
def produce(ctx, impl_name):
    config = ctx.obj["config"]

    # Parse implementation class name
    try:
        impl_class_name = config["producer"]["implementations"][impl_name]["class"]
    except KeyError:
        raise click.BadParameter(
            param_hint="--impl-name",
            message="supported values are provided with list_impls command",
        )

    # Dynamically import the implementation class
    try:
        module = importlib.import_module("producers")
        impl_class = getattr(module, impl_class_name)
    except (ImportError, AttributeError):
        click.echo(f"ERROR: Could not find implementation class: {impl_class_name}")
        return

    # run producer
    producer = impl_class(config)
    producer.run()


@cli.command()
@click.pass_context
def list_impls(ctx):
    impls = ctx.obj["config"]["producer"]["implementations"]
    for impl_name, impl_details in impls.items():
        click.echo(f"{impl_name:<20} -- {impl_details['description']}")


if __name__ == "__main__":
    cli()
