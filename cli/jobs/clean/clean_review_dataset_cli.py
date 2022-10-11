import datetime
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
def clean_weekly_review_cli(year: int, week_number: int):
    """Clean and store daily review files for one set week

    :param year: year of weekly cleaning reviews
    :param week_number:  number of week starts from January
    :return:
    """
    start_date, end_date = get_date_range(year=year, number_of_week=week_number)

    log.info(f"Run weekly cleaning and distributing `review` dataset for {week_number} week ({start_date} - {end_date}) {year} year...")
    for _date in [start_date + timedelta(days=x) for x in range(7)]:
        clean_review_job = YelpReviewDatasetStagingJob()
        clean_review_job.run(date=_date)

    log.info(f"Weakly cleaning review dataset done...")


@cli.command()
@click.option('--date', '-d', type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
def clean_daily_review_cli(date: datetime):
    """Clean and store daily review files for one day

    :param date: clean review on this day
    :return:
    """

    log.info(f"Run cleaning and `review` dataset for {date} date...")
    clean_review_job = YelpReviewDatasetStagingJob()
    clean_review_job.run(date=date.date())

    log.info(f"Cleaning review dataset done...")
