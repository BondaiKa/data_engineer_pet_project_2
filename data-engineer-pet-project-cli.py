import logging

import click

from cli.jobs.clean.clean_business_dataset_cli import cli as clean_yelp_business_datasets_cli
from cli.jobs.clean.clean_checkin_dataset_cli import cli as clean_checkin_dataset
from cli.jobs.clean.clean_review_dataset_cli import cli as weekly_clean_review_cli
from cli.jobs.clean.clean_tip_dataset_cli import cli as clean_tip_datasets_cli
from cli.jobs.clean.clean_user_dataset_cli import cli as clean_user_dataset_cli
from cli.load.load_yelp_dataset_locally_cli import cli as load_yelp_dataset_cli

cli = click.CommandCollection(sources=[
    load_yelp_dataset_cli,
    clean_yelp_business_datasets_cli,
    weekly_clean_review_cli,
    clean_user_dataset_cli,
    clean_checkin_dataset,
    clean_tip_datasets_cli,
])

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)
    cli()
