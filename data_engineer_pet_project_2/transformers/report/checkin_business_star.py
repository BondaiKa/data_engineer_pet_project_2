from pyspark.sql import DataFrame, functions as f


def get_business_checkins_with_star_report(
        business_df: DataFrame, checkin_df: DataFrame,
        business_name: str, stars: str, number_checkin: str) -> DataFrame:
    """get business checkin with overall and weekly stars dataset

    :param business_df:  business dataset
    :param checkin_df: checkin dataset
    :param business_name: business company name
    :param stars: overall stars of business
    :param number_checkin: total number of checkin
    :return: joined business checkin report df
    """
    return checkin_df \
        .join(f.broadcast(business_df),
              business_df.business_id == checkin_df.business_id,
              how="inner"
              ) \
        .select(f.col(business_name), f.col(stars), f.col(number_checkin)) \
        .orderBy([f.col(stars), f.col(number_checkin)], ascending=False)
