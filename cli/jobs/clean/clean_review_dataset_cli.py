import logging
from datetime import datetime, timedelta

import click

from data_engineer_pet_project_2.base.utils import get_date_range
from data_engineer_pet_project_2.jobs.cleaning.clean_review_dataset import YelpReviewDatasetStagingJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--year_date', '-d', type=click.DateTime(format(["%Y"])), required=True)
@click.option('--week_number', '-d', type=click.INT, required=True)
def weekly_clean_review_cli(year_date: datetime, week_number: int):
    """Clean and store daily review files for one set week

    :param year_date: year
    :param week_number:  number of week starts from January
    :return:
    """
    start_date, end_date = get_date_range(input_datetime=year_date, week_number=week_number)

    log.info(f"Run weekly cleaning and distributing `review` dataset for {week_number} week {year_date} year...")
    for cur_date in [start_date + timedelta(days=x) for x in range(6)]:
        weather_dataset_job = YelpReviewDatasetStagingJob()
        weather_dataset_job.run(date=cur_date)

    log.info(f"`Weakly cleaning review dataset done...")
