import logging
from datetime import timedelta

import click

from data_engineer_pet_project_2.base.utils import get_date_range
from data_engineer_pet_project_2.jobs.cleaning.clean_review_dataset import YelpReviewDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--year', '-y', type=click.INT, required=True)
@click.option('--week_number', '-w', type=click.INT, required=True)
def weekly_clean_review_cli(year: int, week_number: int):
    """Clean and store daily review files for one set week

    :param year: year of weekly cleaning reviews
    :param week_number:  number of week starts from January
    :return:
    """
    start_date, end_date = get_date_range(year=year, number_of_week=week_number)

    log.info(f"Run weekly cleaning and distributing `review` dataset for {week_number} week {year} year...")
    for cur_date in [start_date + timedelta(days=x) for x in range(6)]:
        clean_review_job = YelpReviewDatasetStagingJob()
        clean_review_job.run(date=cur_date)

    log.info(f"`Weakly cleaning review dataset done...")
