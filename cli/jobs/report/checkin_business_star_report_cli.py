import logging

import click

from data_engineer_pet_project_2.jobs.report.checkin_business_star_report import YelpCheckinBusinessStarReportJob

log = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', level=logging.INFO)


@cli.command()
def create_report_checkin_business_over_start_cli():
    """Create checkin business overall star rating report

    Create number of checkins of a business compared to the overall star rating report
    """
    log.info(f"Start to make checkin business overall star rating report...")
    checkin_business_star_report_job = YelpCheckinBusinessStarReportJob()
    checkin_business_star_report_job.run()
    log.info(f"checkin business overall star rating report has been created...")
