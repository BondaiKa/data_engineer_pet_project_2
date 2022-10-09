import logging

import click

from data_engineer_pet_project_2.jobs.cleaning.clean_business_dataset import YelpBusinessDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def clean_yelp_business_datasets_cli():
    """clean Yelp business dataset"""
    log.info(f"Start to clean yelp business dataset...")
    clean_business_job = YelpBusinessDatasetStagingJob()
    clean_business_job.run()
    log.info(f"Yelp business dataset has been cleaned...")
