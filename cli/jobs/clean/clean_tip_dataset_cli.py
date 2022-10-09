import logging

import click

from data_engineer_pet_project_2.jobs.cleaning.clean_tip_dataset import YelpTipDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def clean_tip_datasets_cli():
    """Clean tip dataset"""
    log.info(f"Start to clean `tip` dataset...")
    tip_clean_job = YelpTipDatasetStagingJob()
    tip_clean_job.run()
    log.info(f"Cleaning `tip` dataset has been completed...")
