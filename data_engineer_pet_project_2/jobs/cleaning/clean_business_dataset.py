from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.landing import BaseLandingArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.base import BaseJob
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.business import YelpBusinessDatasetSchema
from data_engineer_pet_project_2.transformers.cleaning.clean_business_dataset import clean_business_dataset


class YelpBusinessDatasetStagingJob(BaseJob):
    """Clean and convert business dataset"""
    area = BaseStagingArea()
    schema = YelpBusinessDatasetSchema

    def extract(self, *args, **kwargs) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_json_file(paths=self._get_initial_dataset_paths())
        )

    def _get_initial_dataset_paths(self, *args, **kwargs):
        return BaseLandingArea().get_landing_raw_yelp_dataset_business_json_path()

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def filter_df(self, dataset: DataFrame, *args, **kwargs) -> DataFrame:
        return clean_business_dataset(
            df=dataset, address=self.schema.address, business_id=self.schema.business_id,
            categories=self.schema.categories, city=self.schema.city,
            is_open=self.schema.is_open, stars=self.schema.stars, name=self.schema.name,
            review_count=self.schema.review_count
        )

    def save(self, df: DataFrame, *args, **kwargs):
        for path in self.area.get_staging_business_dataset_path():
            df.repartition(1).write.mode('overwrite').parquet(path=path)
