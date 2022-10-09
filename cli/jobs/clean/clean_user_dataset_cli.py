import logging

import click

from data_engineer_pet_project_2.jobs.cleaning.clean_user_dataset import YelpUserDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def clean_user_dataset_cli():
    """Clean `user` dataset"""
    log.info(f"Clean `user` dataset...")
    user_dataset_job = YelpUserDatasetStagingJob()
    user_dataset_job.run()
    log.info(f"`Cleaning `user` dataset operation has been completed...")
