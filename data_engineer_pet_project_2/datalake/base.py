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

    def add_dataset_name_prefix(self, path: Union[str, Path], dataset_name: str):
        """Add dataset name prefix"""
        return f"{dataset_name}/{path}"

    def add_base_prefix(self, path: Union[str, Path]):
        return f"{Config().dataset_core_path}/{path}"

    def get_full_paths(self, paths: Iterable[Union[str, Path]], dataset_name: str) -> List[str]:
        """Add base directory and area prefixes"""
        return [
            self.add_base_prefix(
                self.add_container_area_prefix(
                    self.add_dataset_name_prefix(path, dataset_name=dataset_name))) for path in paths]
