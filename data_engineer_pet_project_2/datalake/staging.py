import datetime
from datetime import timedelta
from typing import List

from data_engineer_pet_project_2.base.utils import (
    YELP_BUSINESS_NAME, YELP_CHECKIN_NAME,
    YELP_REVIEW_NAME, YELP_TIP_NAME,
    YELP_USER_NAME,
)
from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea


class BaseStagingArea(BaseDataLakeArea):
    """Staging area for saving clean datasets"""
    schemas = None
    AREA_CONTAINER = 'staging'

    def get_staging_business_dataset_path(self):
        """cleaned business dataset path"""
        filename = f"{YELP_BUSINESS_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_BUSINESS_NAME)

    def get_staging_review_dataset_paths(self, start_date: datetime, end_date: datetime) -> List[str]:
        """cleaned weekly review dataset paths"""
        delta = end_date - start_date
        return [self.get_staging_review_dataset_path(date=start_date + timedelta(days=i))
                for i in range(delta.days + 1)]

    def get_staging_review_dataset_path(self, date: datetime.date) -> str:
        """cleaned review dataset path for specific day"""
        filename = f"{date}-{YELP_REVIEW_NAME}.parquet"
        return self.get_full_split_by_year_month_path(path=filename,
                                                      dataset_name=YELP_REVIEW_NAME,
                                                      date=date)

    def get_staging_user_dataset_path(self):
        """cleaned user dataset path"""
        filename = f"{YELP_USER_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_USER_NAME)

    def get_staging_checkin_dataset_path(self):
        """cleaned checkin dataset path"""
        filename = f"{YELP_CHECKIN_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_CHECKIN_NAME)

    def get_staging_tip_dataset_path(self):
        """cleaned tip dataset path"""
        filename = f"{YELP_TIP_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_TIP_NAME)
