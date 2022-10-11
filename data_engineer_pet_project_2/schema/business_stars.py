from data_engineer_pet_project_2.schema.business import YelpBusinessDatasetSchema
from data_engineer_pet_project_2.schema.review import YelpReviewDatasetSchema


class YelpPeriodBusinessStarReportSchema:
    business_name = YelpBusinessDatasetSchema.name
    stars = YelpReviewDatasetSchema.stars
    business_stars_average = 'overall_stars'
