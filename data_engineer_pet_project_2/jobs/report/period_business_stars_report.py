import logging
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.public import BasePublicArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.business_stars import YelpPeriodBusinessStarReportSchema
from data_engineer_pet_project_2.transformers.period_business_star import get_period_business_star_report

log = logging.getLogger(__name__)


class YelpPeriodBusinessStarReportJob:
    """Clean and convert review dataset"""
    area = BasePublicArea()
    schema = YelpPeriodBusinessStarReportSchema

    def extract(self, start_date: datetime.date, end_date: datetime.date) -> Tuple[DataFrame, DataFrame]:
        """Load dataset"""
        review_df = self.filter_df(
            dataset=Session().load_dataframe(
                paths=BaseStagingArea().get_staging_review_dataset_paths(start_date=start_date, end_date=end_date))
        )
        business_df = self.filter_df(
            dataset=Session().load_dataframe(
                paths=BaseStagingArea().get_staging_business_dataset_path())
        )

        return business_df, review_df

    def transform(self, business_df: DataFrame, review_df: DataFrame) -> DataFrame:
        return get_period_business_star_report(
            business_df=business_df, review_df=review_df,
            business_name=self.schema.business_name, stars=self.schema.stars,
            business_star_average=self.schema.business_stars_average
        )

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def run(self, start_date: datetime.date, end_date: datetime.date):
        """Run extracting, transforming and saving dataframe job"""
        log.info(f'Start to extract data...')
        business_df, review_df = self.extract(start_date=start_date, end_date=end_date)

        log.info(f'Start dataframe transformation...')
        df = self.transform(business_df=business_df, review_df=review_df)

        log.info(f'Start save transformed results...')
        self.save(df, date=start_date)

    def save(self, df: DataFrame, date: datetime):
        df.repartition(1).write.mode('overwrite') \
            .csv(path=self.area.get_public_weekly_business_stars_report_path(date), header=True)
