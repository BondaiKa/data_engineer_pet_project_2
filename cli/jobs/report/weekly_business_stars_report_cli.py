import logging

import click

from data_engineer_pet_project_2.base.utils import get_date_range
from data_engineer_pet_project_2.jobs.report.period_business_stars_report import YelpPeriodBusinessStarReportJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
@click.option('--year', '-y', type=click.INT, required=True)
@click.option('--week_number', '-w', type=click.INT, required=True)
def create_report_weekly_stars_business_cli(year: int, week_number: int):
    """Create stars per business on a weekly basis report

    :param year: year of weekly cleaning reviews
    :param week_number:  number of week starts from January
    :return:
    """
    start_date, end_date = get_date_range(year=year, number_of_week=week_number)

    log.info(f"Run stars per business on a weekly basis report for {week_number} week "
             f"({start_date} - {end_date}) {year} year...")

    weekly_business_report_job = YelpPeriodBusinessStarReportJob()
    weekly_business_report_job.run(start_date=start_date, end_date=end_date)

    log.info(f"Weekly business star report has been completed...")
