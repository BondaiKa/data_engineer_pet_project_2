from pyspark.sql import DataFrame, functions as f


def clean_checkin_dataset(df: DataFrame, business_id: str, date: str, result_date_count_field: str = 'count'):
    """Clean and transform checking dataset"""
    return df \
        .withColumn(date, f.explode(f.split(f.col(date), pattern=", "))) \
        .select(f.col(business_id), f.col(date)) \
        .groupby(business_id) \
        .agg(f.count(f.col(date)).alias(result_date_count_field))


def check_business_id_checkin_duplicate(df: DataFrame, business_id: str, date: str):
    """Check that business_id is unique in checkin dataset"""
    return df \
        .select(f.col(business_id), f.col(date)) \
        .groupby(business_id) \
        .count() \
        .where('count > 1') \
        .sort('count', ascending=False)
