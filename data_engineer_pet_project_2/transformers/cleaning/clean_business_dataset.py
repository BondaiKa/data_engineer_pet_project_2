from pyspark.sql import DataFrame, functions as f


def clean_business_dataset(
        df: DataFrame, address: str, business_id: str, categories: str, city: str,
        is_open: str, stars: str, name: str, review_count: str) -> DataFrame:
    """Clean initial business dataset

    clean business dataset and remain only required columns for further analysis
    :param df: dataset
    :param address: business address
    :param business_id: business id
    :param categories: business categories
    :param city: business city
    :param is_open: business open or closed forever
    :param stars: business stars
    :param name: business name
    :param review_count: business review count
    :return: cleaned dataframe
    """
    return df \
        .select(f.col(address), f.col(business_id), f.col(categories), f.col(city),
                f.col(is_open), f.col(stars), f.col(name), f.col(review_count)) \
        .dropna(subset=(is_open, stars, review_count, name)) \
        .filter(df.is_open == 1) \
        .drop(is_open) \
        .dropDuplicates(subset=[business_id])
