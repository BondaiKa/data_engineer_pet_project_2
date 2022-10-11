from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.landing import BaseLandingArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.job_base import BaseJob
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.checkin_schema import YelpCheckinDatasetSchema
from data_engineer_pet_project_2.transformers.cleaning.clean_checkin_dataset import clean_checkin_dataset


class YelpCheckinDatasetStagingJob(BaseJob):
    """Clean and convert checkin dataset"""
    area = BaseStagingArea()
    schema = YelpCheckinDatasetSchema

    def extract(self, *args, **kwargs) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_json_file(paths=self._get_initial_dataset_paths())
        )

    def _get_initial_dataset_paths(self, *args, **kwargs):
        return BaseLandingArea().get_landing_raw_yelp_dataset_checkin_json_path()

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return clean_checkin_dataset(df=df,
                                     business_id=self.schema.business_id,
                                     date=self.schema.date,
                                     result_date_count_field=self.schema.number_of_checkins)

    def filter_df(self, dataset: DataFrame, *args, **kwargs) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, *args, **kwargs):
        for path in self.area.get_staging_checkin_dataset_path():
            df.repartition(1).write.mode('overwrite').parquet(path=path)
