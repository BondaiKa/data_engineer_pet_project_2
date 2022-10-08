from datetime import datetime

from data_engineer_pet_project_2.base.utils import YELP_BUSINESS_NAME
from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea


class BaseStagingArea(BaseDataLakeArea):
    schemas = None
    AREA_CONTAINER = 'staging'

    def get_staging_business_dataset_paths(self, date: datetime):
        filename = f"{YELP_BUSINESS_NAME}.parquet"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_BUSINESS_NAME, date=date)
