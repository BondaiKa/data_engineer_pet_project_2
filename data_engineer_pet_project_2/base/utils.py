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
