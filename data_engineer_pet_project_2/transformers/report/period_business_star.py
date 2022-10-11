from pyspark.sql import DataFrame, functions as f


def get_period_business_star_report(
        business_df: DataFrame, review_df: DataFrame,
        business_name: str, stars: str, business_overall_stars: str) -> DataFrame:
    """Get period business report

    Create period business report with period stars
    (could be weekly, monthly, yearly) and overall stars

    :param business_df: business dataset
    :param review_df:  review dataset
    :param business_name: business company name
    :param stars: stars by given period
    :param business_overall_stars: business overall stars
    :return: period report with the stars
    """
    return review_df \
        .join(f.broadcast(business_df.withColumnRenamed(stars, business_overall_stars)),
              business_df.business_id == review_df.business_id,
              how="inner"
              ) \
        .select(f.col(business_name), f.col(stars), f.col(business_overall_stars)) \
        .orderBy([f.col(stars)], ascending=False)
