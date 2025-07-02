import click
from data_creation.initial_demo import initial_incremental_demo
from data_creation.incremental_data_creation.training_data_generator import (
    generate_training_data,
)
from data_creation.incremental_data_creation.validation_data_generator import (
    generate_validation_data,
)
from data_creation.incremental_data_creation.marketing_data_generator import (
    generate_marketing_data,
)
from data_creation.incremental_data_creation.ads_data_generator import (
    generate_ads_data,
)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--days-back", default=30, help="Amount of days of tests", type=int)
@click.option("--target", help="Target environment", type=str)
@click.option("--profiles-dir", help="Profiles directory", type=str)
def initial_incremental_demo_flow(days_back, target, profiles_dir):
    initial_incremental_demo(
        days_back=days_back, target=target, profiles_dir=profiles_dir
    )


@cli.command()
@click.option(
    "--data-to-generate",
    default="all",
    help="Which data to generate - training / validation / marketing / ads / all",
    type=click.Choice(["training", "validation", "marketing", "ads", "all"]),
)
def generate_new_data(data_to_generate):
    if data_to_generate == "training":
        generate_training_data()
    elif data_to_generate == "validation":
        generate_validation_data()
    elif data_to_generate == "marketing":
        generate_marketing_data()
    elif data_to_generate == "ads":
        generate_ads_data()
    else:
        generate_training_data()
        generate_validation_data()
        generate_marketing_data()


if __name__ == "__main__":
    cli()
