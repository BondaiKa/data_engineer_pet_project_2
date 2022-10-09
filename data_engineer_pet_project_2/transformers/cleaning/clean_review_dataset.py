from datetime import datetime

from pyspark.sql import DataFrame, functions as f


def clean_review_dataset(df: DataFrame, review_id, user_id: str, business_id: str, stars: str,
                         date: str, filter_condition_date: datetime):
    """Clean convert and filtered by date review dataset

    :param df: review dataset
    :param review_id:  review id
    :param user_id: user id
    :param business_id:  business id
    :param stars: review stars
    :param date: date in review dataset
    :param filter_condition_date: filter review dataset by this date
    :return: cleaned and filtered dataset
    """
    return df \
        .select(f.col(review_id), f.col(user_id), f.col(business_id), f.col(stars), f.col(date)) \
        .withColumn(date, f.date_format(date, "yyyy-MM-dd")) \
        .withColumn(stars, f.col(stars).cast("Float")) \
        .where(df.date == filter_condition_date) \
        .drop(date) \
        .dropna(subset=(review_id, user_id, business_id, stars)) \
        .dropDuplicates(subset=[review_id])
