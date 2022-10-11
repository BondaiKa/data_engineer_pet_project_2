from pyspark.sql import DataFrame, functions as f


def get_business_checkins_with_star_report(business_df: DataFrame, checkin_df: DataFrame,
                                           business_name: str, stars: str, number_checkin: str) -> DataFrame:
    return checkin_df \
        .join(f.broadcast(business_df),
              business_df.business_id == checkin_df.business_id,
              how="inner"
              ) \
        .select(f.col(business_name), f.col(stars), f.col(number_checkin)) \
        .orderBy([f.col(stars), f.col(number_checkin)], ascending=False)
