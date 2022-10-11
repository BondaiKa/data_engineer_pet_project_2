import datetime

from data_engineer_pet_project_2.base.utils import YELP_CHECKIN_BUSINESS_OVERALL_STAR_REPORT, \
    YELP_WEEKLY_BUSINESS_STAR_REPORT
from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea


class BasePublicArea(BaseDataLakeArea):
    AREA_CONTAINER = 'public'

    def get_public_checkin_business_star_path(self):
        filename = f"{YELP_CHECKIN_BUSINESS_OVERALL_STAR_REPORT}.csv"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_CHECKIN_BUSINESS_OVERALL_STAR_REPORT)[0]

    def get_public_weekly_business_stars_report_path(self, date: datetime.date):
        week_number = date.isocalendar()[1]
        filename = f"{week_number}-{YELP_WEEKLY_BUSINESS_STAR_REPORT}.csv"
        return self.get_full_split_by_year_path(path=filename, dataset_name=YELP_WEEKLY_BUSINESS_STAR_REPORT,
                                                date=date)
