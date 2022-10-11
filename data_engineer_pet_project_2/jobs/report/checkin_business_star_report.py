import logging
from datetime import datetime
from typing import List, Tuple

from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.public import BasePublicArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.checkin_business_star_schema import YelpCheckinBusinessStarReportSchema
from data_engineer_pet_project_2.transformers.report.checkin_business_star import get_business_checkins_with_star_report

log = logging.getLogger(__name__)


class YelpCheckinBusinessStarReportJob:
    """Clean and convert review dataset"""
    area = BasePublicArea()
    schema = YelpCheckinBusinessStarReportSchema

    def extract(self) -> Tuple[DataFrame, DataFrame]:
        """Load dataset"""
        checkin_df = self.filter_df(
            dataset=Session().load_dataframe(
                paths=BaseStagingArea().get_staging_checkin_dataset_path())
        )
        business_df = self.filter_df(
            dataset=Session().load_dataframe(
                paths=BaseStagingArea().get_staging_business_dataset_path())
        )

        return business_df, checkin_df

    def _get_initial_dataset_paths(self, start_date: datetime, end_date: datetime,
                                   *args, **kwargs) -> List[str]:
        return BaseStagingArea().get_staging_review_dataset_paths(
            start_date=start_date, end_date=end_date)

    def transform(self, business_df: DataFrame, checkin_df: DataFrame, *args, **kwargs) -> DataFrame:
        return get_business_checkins_with_star_report(
            business_df=business_df, checkin_df=checkin_df, business_name=self.schema.business_name,
            stars=self.schema.stars, number_checkin=self.schema.number_checkin
        )

    def filter_df(self, dataset: DataFrame) -> DataFrame:
        return dataset

    def run(self):
        """Run extracting, transforming and saving dataframe job"""
        log.info('Start to extract data...')
        business_df, checkin_df = self.extract()

        log.info('Start dataframe transformation...')
        df = self.transform(business_df=business_df, checkin_df=checkin_df)

        log.info('Start save transformed results...')
        self.save(df)

    def save(self, df: DataFrame):
        df.repartition(1).write.mode('overwrite').csv(
            path=self.area.get_public_checkin_business_star_path(),
            header=True
        )
