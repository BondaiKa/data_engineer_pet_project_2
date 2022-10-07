from pathlib import Path
from typing import Optional, Union



from data_engineer_pet_project_2.base.utils import YELP_KAGGLE_DATASET_NAME
from data_engineer_pet_project_2.config import Config
from data_engineer_pet_project_2.datalake.landing import BaseLandingArea


def load_dataset_from_kaggle(dataset_name: str, download_path: Union[str, Path]):
    """Load a dataset from kaggle"""
    import kaggle
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=download_path,
                                      unzip=True)


def load_yelp_dataset_locally():
    """Load yelp dataset locally"""
    dataset_full_path = Config().dataset_core_path / BaseLandingArea.AREA_CONTAINER
    load_dataset_from_kaggle(dataset_name=YELP_KAGGLE_DATASET_NAME, download_path=dataset_full_path)
