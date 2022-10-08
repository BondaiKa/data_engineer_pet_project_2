import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame

from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea
from data_engineer_pet_project_2.jobs.session import Session

log = logging.getLogger(__name__)


class BaseJob(metaclass=ABCMeta):
    """Base dataset job worker"""
    area: BaseDataLakeArea
    schema: type

    def extract(self, start_date: Optional[datetime], end_date: datetime, *args, **kwargs) -> DataFrame:
        """Load dataset"""
        return self.filter_df(
            dataset=Session().load_dataframe(
                paths=self._get_initial_dataset_paths(start_date=start_date, end_date=end_date))
        )

    def _get_initial_dataset_paths(self, start_date: Optional[datetime], end_date: datetime, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def transform(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        """Apply transformations"""
        raise NotImplementedError

    @abstractmethod
    def save(self, df: DataFrame, *args, **kwargs):
        """Save results"""
        raise NotImplementedError

    def run(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, *args, **kwargs):
        """run extracting, transforming and saving dataframe job"""
        log.info(f'Start to extract data...')
        df = self.extract(start_date=start_date, end_date=end_date)

        log.info(f'Start dataframe transformation...')
        df = self.transform(df)

        log.info(f'Start save transformed results...')
        self.save(df, *args, **kwargs)

    @abstractmethod
    def filter_df(self, dataset: DataFrame, *args, **kwargs) -> DataFrame:
        """Filter dataset"""
        raise NotImplementedError
