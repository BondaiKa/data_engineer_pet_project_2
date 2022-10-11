from pyspark.sql import DataFrame, functions as f


def clean_tip_dataset(df: DataFrame, date: str, compliment_count: str, business_id: str, user_id: str) -> DataFrame:
    """Clean and tip dataset"""
    return df \
        .withColumn(date, f.to_date(f.col(date), "MM-dd-yyyy")) \
        .withColumn(compliment_count, f.col(compliment_count).cast('Int')) \
        .select(f.col(date), f.col(compliment_count), f.col(business_id), f.col(user_id)) \
        .dropna(subset=(business_id, user_id))
