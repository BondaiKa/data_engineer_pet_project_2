import logging
from pathlib import Path
from typing import Any, Dict

from data_engineer_pet_project_2.base import MetaSingleton
from data_engineer_pet_project_2.config.reader import YmlConfigReader

log = logging.getLogger(__name__)


class Config(metaclass=MetaSingleton):
    """Configuration class that read configs from a yml file"""

    def __init__(self, cfg=None):
        self._cfg: Dict[str, Any] = cfg if cfg else YmlConfigReader().read()
        self.datasets = self._cfg.get('datasets', dict())
        self.hdfs = self._cfg.get('hdfs', dict())

    @property
    def spark_conf(self) -> Dict[str, str]:
        return self._cfg.get('spark', dict())

    @property
    def dataset_core_path(self) -> Path:
        return Path(self.hdfs.get("dataset_core_path"))

    @property
    def yelp_dataset_name(self) -> str:
        return "yelp-dataset"
