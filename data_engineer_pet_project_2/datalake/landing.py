from data_engineer_pet_project_2.base.utils import (YELP_BUSINESS_NAME, YELP_CHECKIN_NAME, YELP_REVIEW_NAME,
                                                    YELP_TIP_NAME, YELP_USER_NAME)
from data_engineer_pet_project_2.datalake.base import BaseDataLakeArea


class BaseLandingArea(BaseDataLakeArea):
    AREA_CONTAINER = 'landing'

    def get_landing_raw_yelp_dataset_business_json_path(self) -> str:
        """Get initial yelp dataset business path"""
        filename = f"{YELP_BUSINESS_NAME}.json"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_BUSINESS_NAME)[0]

    def get_landing_raw_yelp_dataset_checkin_json_path(self) -> str:
        """Get initial yelp dataset checkin path"""
        filename = f"{YELP_CHECKIN_NAME}.json"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_CHECKIN_NAME)[0]

    def get_landing_raw_yelp_dataset_review_json_path(self) -> str:
        """Get initial yelp dataset review path"""
        filename = f"{YELP_REVIEW_NAME}.json"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_REVIEW_NAME)[0]

    def get_landing_raw_yelp_dataset_tip_json_path(self) -> str:
        """Get initial yelp dataset tip path"""
        filename = f"{YELP_TIP_NAME}.json"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_TIP_NAME)[0]

    def get_landing_raw_yelp_dataset_user_json_path(self) -> str:
        """Get initial yelp dataset user path"""
        filename = f"{YELP_USER_NAME}.json"
        return self.get_full_paths(paths=[filename], dataset_name=YELP_USER_NAME)[0]
