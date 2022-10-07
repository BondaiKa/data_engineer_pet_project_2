from typing import List, Union

from data_engineer_pet_project_2.base import MetaSingleton
from data_engineer_pet_project_2.config import Config
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession


class Session(metaclass=MetaSingleton):
    """Spark session handler"""

    def __init__(self, spark_session=None):
        self._ssc = spark_session

    def build_session(self):
        builder = SparkSession.builder
        for param, value in Config().spark_conf.items():
            builder = builder.config(param, value)
        self._ssc = builder.getOrCreate()

    @property
    def spark_session(self) -> SparkSession:
        if self._ssc is None:
            self.build_session()
        return self._ssc

    def load_dataframe(self, paths: Union[List[str], str], **options) -> SparkDataFrame:
        """Return dataframe from path with given options."""
        return self.spark_session.read.load(paths, **options)

    def load_json_file(self, paths: Union[List[str], str], **options) -> SparkDataFrame:
        """Return dataframe from path with given options."""
        return self.spark_session.read.json(paths, **options)
