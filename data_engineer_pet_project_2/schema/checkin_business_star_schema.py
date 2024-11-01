from data_engineer_pet_project_2.schema.business_schema import YelpBusinessDatasetSchema
from data_engineer_pet_project_2.schema.checkin_schema import YelpCheckinDatasetSchema


class YelpCheckinBusinessStarReportSchema:
    business_id = YelpBusinessDatasetSchema.business_id
    business_name = YelpBusinessDatasetSchema.name
    stars = YelpBusinessDatasetSchema.stars
    number_checkin = YelpCheckinDatasetSchema.number_of_checkins
