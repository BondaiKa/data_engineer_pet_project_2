import logging

import click

from data_engineer_pet_project_2.jobs.cleaning.clean_checkin_dataset import YelpCheckinDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def clean_checkin_dataset_cli():
    """Clean `checkin` dataset"""
    log.info(f"Run cleaning checkin dataset...")
    checkin_dataset_job = YelpCheckinDatasetStagingJob()
    checkin_dataset_job.run()
    log.info(f"Cleaning `checkin` dataset has been completed...")
