import logging
from datetime import datetime

from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.landing import BaseLandingArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.job_base import BaseJob
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.review_schema import YelpReviewDatasetSchema
from data_engineer_pet_project_2.transformers.cleaning.clean_review_dataset import clean_review_dataset

log = logging.getLogger(__name__)


class YelpReviewDatasetStagingJob(BaseJob):
    """Clean and convert review dataset"""
    area = BaseStagingArea()
    schema = YelpReviewDatasetSchema

    def extract(self, date: datetime, *args, **kwargs) -> DataFrame:
        """Load dataset

        :param date: for getting review on this date
        :return:
        """
        return self.filter_df(
            dataset=Session().load_json_file(paths=self._get_initial_dataset_paths()),
            date=date,
        )

    def _get_initial_dataset_paths(self, *args, **kwargs):
        return BaseLandingArea().get_landing_raw_yelp_dataset_review_json_path()

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame, date: datetime, *args, **kwargs) -> DataFrame:
        return clean_review_dataset(df=dataset, review_id=self.schema.review_id, user_id=self.schema.user_id,
                                    business_id=self.schema.business_id, stars=self.schema.stars,
                                    date=self.schema.date, filter_condition_date=date)

    def run(self, date: datetime, *args, **kwargs):
        """run extracting, transforming and saving dataframe job"""
        log.info(f'Start to extract data for {date}...')
        df = self.extract(date)

        log.info('Start dataframe transformation...')
        df = self.transform(df, date=date)

        log.info('Start save transformed results...')
        self.save(df, date, *args, **kwargs)

    def save(self, df: DataFrame, date: datetime, *args, **kwargs):
        df.repartition(1).write.mode('overwrite').parquet(
            path=self.area.get_staging_review_dataset_path(date=date)
        )
