import logging

import click

from data_engineer_pet_project_2.load.load_yelp_dataset import load_yelp_dataset_locally

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def load_yelp_dataset_locally_cli() -> None:
    """Load yelp dataset locally cli command

    :return:
    """
    log.info(f"Run `load yelp dataset locally`...")
    load_yelp_dataset_locally()
    log.info("Done...")
