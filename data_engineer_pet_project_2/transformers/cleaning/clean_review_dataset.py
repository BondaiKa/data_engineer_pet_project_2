from pyspark.sql import DataFrame, functions as f


def clean_review_dataset(df: DataFrame, review_id, user_id: str, business_id: str, stars: str, start_date: str,
                         end_date: str):
    """Clean review and filtered by date dataset

    :param df: review dataset
    :param review_id:  review id
    :param user_id: user id
    :param business_id:  business id
    :param stars: review stars
    :param end_date: date
    :return: cleaned and filtered dataset
    """
    return df \
        .select(f.col(review_id), f.col(user_id), f.col(business_id), f.col(stars), f.col(end_date)) \
        .
