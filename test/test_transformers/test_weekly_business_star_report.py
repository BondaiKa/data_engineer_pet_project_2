import pytest
from pyspark.sql import Row

from data_engineer_pet_project_2.schema.business_stars_schema import YelpPeriodBusinessStarReportSchema
from data_engineer_pet_project_2.transformers.report.period_business_star import get_period_business_star_report


@pytest.fixture()
def business_dataframe(spark_session):
    # TODO:
    return spark_session.createDataFrame([
        Row(address='4903 State Rd 54', business_id='X-dPdw1PluJnT-fpiWrfKw',
            categories='Seafood, Restaurants, Latin American, Food, Food Trucks', city='New Port Richey', stars=4.5,
            name="Frankie's Raw Bar", review_count=24),
        Row(address='6537 Gunn Hwy', business_id='--another_id',
            categories='Hair Salons, Hair Stylists, Beauty & Spas', city='Tampa', stars=5.0, name='Studio G Salon',
            review_count=8)
    ])


@pytest.fixture()
def review_dataframe(spark_session):
    # TODO:
    return spark_session.createDataFrame([
        Row(review_id='---4VcQZzy_vIIifUDqxsg', user_id='EopuF3BhVXAGJWEje_TJ-g', business_id='X-dPdw1PluJnT-fpiWrfKw',
            stars=1.0),
        Row(review_id='-36MNd2nTl8veaOOzOF5ow', user_id='pbu2vw2qHiB7KlVIz-GuEQ', business_id='another_id_2',
            stars=5.0),
    ]
    )


def test_positive_weekly_business_star_report(business_dataframe, review_dataframe):
    df = get_period_business_star_report(
        business_df=business_dataframe, review_df=review_dataframe,
        business_name=YelpPeriodBusinessStarReportSchema.business_name,
        stars=YelpPeriodBusinessStarReportSchema.stars,
        business_overall_stars=YelpPeriodBusinessStarReportSchema.business_stars_average
    )
    test_case = [
        ("Frankie's Raw Bar", 1.0, 4.5),
    ]
    assert sorted(
        [(row.name, row.stars, row.overall_stars) for row in df.collect()],
        key=lambda x: (x[1])) == test_case
