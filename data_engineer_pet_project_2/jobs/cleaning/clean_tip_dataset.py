from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.landing import BaseLandingArea
from data_engineer_pet_project_2.datalake.staging import BaseStagingArea
from data_engineer_pet_project_2.jobs.base import BaseJob
from data_engineer_pet_project_2.jobs.session import Session
from data_engineer_pet_project_2.schema.tip import YelpTipDatasetSchema
from data_engineer_pet_project_2.transformers.cleaning.clean_tip_dataset import clean_tip_dataset


class YelpTipDatasetStagingJob(BaseJob):
    """Clean and convert user dataset"""
    area = BaseStagingArea()
    schema = YelpTipDatasetSchema

    def extract(self, *args, **kwargs) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_json_file(paths=self._get_initial_dataset_paths())
        )

    def _get_initial_dataset_paths(self, *args, **kwargs):
        return BaseLandingArea().get_landing_raw_yelp_dataset_tip_json_path()

    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return clean_tip_dataset(df=df, date=self.schema.date, compliment_count=self.schema.compliment_count,
                                 business_id=self.schema.business_id, user_id=self.schema.user_id)

    def filter_df(self, dataset: DataFrame, *args, **kwargs) -> DataFrame:
        return dataset

    def save(self, df: DataFrame, *args, **kwargs):
        for path in self.area.get_staging_tip_dataset_paths():
            df.repartition(1).write.mode('overwrite').parquet(path=path)