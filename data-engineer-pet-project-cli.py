import logging

import click

from cli.jobs.clean.clean_business_dataset_cli import cli as clean_yelp_business_datasets_cli
from cli.jobs.clean.clean_review_dataset_cli import cli as weekly_clean_review_cli
from cli.load.load_yelp_dataset_locally_cli import cli as load_yelp_dataset_cli

cli = click.CommandCollection(sources=[
    load_yelp_dataset_cli,
    clean_yelp_business_datasets_cli,
    weekly_clean_review_cli,
])

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)
    cli()
