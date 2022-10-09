from abc import ABCMeta
from datetime import datetime
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

    def add_dataset_name_prefix(self, path: Union[str, Path], dataset_name: str) -> str:
        """Add dataset name prefix"""
        return f"{dataset_name}/{path}"

    def add_base_prefix(self, path: Union[str, Path]) -> str:
        return f"{Config().dataset_core_path}/{path}"

    def add_date_prefix(self, path: Union[str, Path], date: datetime) -> str:
        """Enrich year and month in path"""
        return f"{date.year}/{date.month:02d}/{path}"

    def get_full_split_by_day_paths(self, paths: Iterable[Union[str, Path]], dataset_name: str, date: datetime) -> List[
        str]:
        """Add base directory and area and dataset and splitted by year and month prefixes"""
        return [
            self.add_base_prefix(
                self.add_container_area_prefix(
                    self.add_dataset_name_prefix(
                        self.add_date_prefix(path, date=date), dataset_name=dataset_name))) for path in paths]

    def get_full_paths(self, paths: Iterable[Union[str, Path]], dataset_name: str) -> List[str]:
        """Add base directory and area and dataset prefixes"""
        return [
            self.add_base_prefix(
                self.add_container_area_prefix(
                    self.add_dataset_name_prefix(path, dataset_name=dataset_name))) for path in paths]
