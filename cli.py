import click
from data_creation.initial_demo import initial_incremental_demo
from data_creation.incremental_data_creation.training_data_generator import (
    generate_training_data,
)
from data_creation.incremental_data_creation.validation_data_generator import (
    generate_validation_data,
)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--days-back", default=30, help="Amount of days of tests", type=int)
def initial_incremental_demo_flow(days_back):
    initial_incremental_demo(days_back=days_back)


@cli.command()
@click.option(
    "--data-to-generate",
    default="all",
    help="Which data to generate - training / validation / all",
    type=click.Choice(["training", "validation", "all"]),
)
def generate_new_data(data_to_generate):
    if data_to_generate == "training":
        generate_training_data()
    elif data_to_generate == "validation":
        generate_validation_data()
    else:
        generate_training_data()
        generate_validation_data()


if __name__ == "__main__":
    cli()
