from pyspark.sql import DataFrame, functions as f


def get_period_business_star_report(business_df: DataFrame, review_df: DataFrame, business_name: str, stars: str,
                                    business_star_average: str) -> DataFrame:
    return review_df \
        .join(f.broadcast(business_df.withColumnRenamed(stars, business_star_average)),
              business_df.business_id == review_df.business_id,
              how="inner"
              ) \
        .select(f.col(business_name), f.col(stars), f.col(business_star_average)) \
        .orderBy([f.col(stars)], ascending=False)
