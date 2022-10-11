from datetime import date, datetime, timedelta
from typing import Tuple


class MetaSingleton(type):
    """Metaclass for create singleton"""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


YELP_KAGGLE_DATASET_NAME = 'yelp-dataset'
YELP_DATASET_CORE_NAME = 'yelp_academic_dataset'
YELP_BUSINESS_NAME = f"{YELP_DATASET_CORE_NAME}_business"
YELP_CHECKIN_NAME = f"{YELP_DATASET_CORE_NAME}_checkin"
YELP_REVIEW_NAME = f"{YELP_DATASET_CORE_NAME}_review"
YELP_TIP_NAME = f"{YELP_DATASET_CORE_NAME}_tip"
YELP_USER_NAME = f"{YELP_DATASET_CORE_NAME}_user"

YELP_CHECKIN_BUSINESS_OVERALL_STAR_REPORT = "checkin_business_overall_star_report"
YELP_WEEKLY_BUSINESS_STAR_REPORT = "weekly_business_star_report"


def get_date_range(year: int, number_of_week: int) -> Tuple[date, date]:
    """Get start and end date from year and week number

    :param year_datetime: year
    :param number_of_week: week number start from January
    :return: desired data range
    """

    d = f"{year}-W{number_of_week}"
    start_date = datetime.strptime(d + '-1', "%Y-W%W-%w").date()
    end_date = start_date + timedelta(days=6)
    return start_date, end_date
