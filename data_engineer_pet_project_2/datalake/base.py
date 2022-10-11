import datetime
from abc import ABCMeta
from pathlib import Path
from typing import Iterable, List, Union

from data_engineer_pet_project_2.config import Config


class BaseDataLakeArea(metaclass=ABCMeta):
    """Base class for all data lake areas that should implement given interface."""
    config = Config()
    AREA_CONTAINER: str
    BASE_PATH: str

    def add_container_area_prefix(self, path: Union[str, Path]) -> str:
        """Add datalake area name """
        return f"{self.AREA_CONTAINER}/{path}"

    @staticmethod
    def add_dataset_name_prefix(path: Union[str, Path], dataset_name: str) -> str:
        """Add dataset name prefix"""
        return f"{dataset_name}/{path}"

    def add_base_prefix(self, path: Union[str, Path]) -> str:
        """Base directory of datalake"""
        return f"{self.config.dataset_core_path}/{path}"

    @staticmethod
    def add_year_prefix(path: Union[str, Path], year: int) -> str:
        """Enrich year and month in path"""
        return f"{year}/{path}"

    @staticmethod
    def add_month_prefix(path: Union[str, Path], month: int) -> str:
        """Enrich year and month in path"""
        return f"{month:02d}/{path}"

    @staticmethod
    def add_year_month_prefix(path: Union[str, Path], year: int, month: int) -> str:
        """Enrich year and month in path"""
        return f"{year}/{month:02d}/{path}"

    def get_full_split_by_year_path(self, path: Union[str, Path], dataset_name: str,
                                    date: datetime.date) -> str:
        """Add base directory and area and dataset and splitted by year and month prefixes"""
        return self.add_base_prefix(
            self.add_container_area_prefix(
                self.add_dataset_name_prefix(
                    self.add_year_prefix(
                        path, year=date.year), dataset_name=dataset_name)))

    def get_full_split_by_year_month_path(self, path: Union[str, Path], dataset_name: str,
                                          date: datetime.date) -> str:
        """Add base directory and area and dataset and splitted by year and month prefixes"""
        return self.get_full_split_by_year_path(self.add_month_prefix(path, month=date.month),
                                                date=date,
                                                dataset_name=dataset_name)

    def get_full_paths(self, paths: Iterable[Union[str, Path]], dataset_name: str) -> List[str]:
        """Add base directory and area and dataset prefixes"""
        return [
            self.add_base_prefix(
                self.add_container_area_prefix(
                    self.add_dataset_name_prefix(
                        path, dataset_name=dataset_name))) for path in paths]
