from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.landing import BaseLandingArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.job_base import BaseJob
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.user_schema import YelpUserDatasetSchema
from data_engineer_pet_project_2.transformers.cleaning.clean_user_dataset import clean_user_dataset


class YelpUserDatasetStagingJob(BaseJob):
    """Clean and convert user dataset"""
    area = BaseStagingArea()
    schema = YelpUserDatasetSchema

    def extract(self, *args, **kwargs) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_json_file(paths=self._get_initial_dataset_paths())
        )

    def _get_initial_dataset_paths(self, *args, **kwargs):
        return BaseLandingArea().get_landing_raw_yelp_dataset_user_json_path()

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame, *args, **kwargs) -> DataFrame:
        return clean_user_dataset(
            df=dataset, user_id=self.schema.user_id, name=self.schema.name,
            review_count=self.schema.review_count,
            useful=self.schema.useful, funny=self.schema.funny, cool=self.schema.cool,
            fans=self.schema.fans, average_stars=self.schema.average_stars,
        )

    def save(self, df: DataFrame, *args, **kwargs):
        for path in self.area.get_staging_user_dataset_path():
            df.repartition(1).write.mode('overwrite').parquet(path=path)
