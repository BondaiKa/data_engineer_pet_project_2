import logging

import click

from data_engineer_pet_project_2.jobs.cleaning.clean_business_dataset import YelpBusinessDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def clean_yelp_business_datasets_cli():
    """Join Weather Citibike datasets"""
    log.info(f"Start to clean yelp business dataset...")
    bike_weather_job = YelpBusinessDatasetStagingJob()
    bike_weather_job.run()
    log.info(f"Yelp business dataset has been cleaned...")
