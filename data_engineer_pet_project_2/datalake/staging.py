from datetime import datetime

from data_engineer_pet_project_2.base.utils import YELP_BUSINESS_NAME, YELP_CHECKIN_NAME, YELP_REVIEW_NAME, \
    YELP_USER_NAME, YELP_TIP_NAME
from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea


class BaseStagingArea(BaseDataLakeArea):
    schemas = None
    AREA_CONTAINER = 'staging'

    def get_staging_business_dataset_paths(self):
        filename = f"{YELP_BUSINESS_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_BUSINESS_NAME)

    def get_staging_review_dataset_paths(self, date: datetime):
        filename = f"{date}-{YELP_REVIEW_NAME}.parquet"
        return self.get_full_split_by_day_paths(paths=[filename], dataset_name=YELP_REVIEW_NAME, date=date)

    def get_staging_user_dataset_paths(self):
        filename = f"{YELP_USER_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_USER_NAME)

    def get_staging_checkin_dataset_paths(self):
        filename = f"{YELP_CHECKIN_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_CHECKIN_NAME)

    def get_staging_tip_dataset_paths(self):
        filename = f"{YELP_TIP_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_TIP_NAME)

