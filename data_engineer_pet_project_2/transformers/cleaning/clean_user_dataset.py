from pyspark.sql import DataFrame, functions as f


def clean_user_dataset(df: DataFrame, user_id: str, name: str, review_count: str, useful: str,
                       funny: str, cool: str, fans: str,
                       average_stars: str):
    """Clean and converty column type of user dataset

    :param df: user dataset
    :param user_id:  user id
    :param name: user's name
    :param review_count: number of review
    :param useful: number of useful votes sent by the user
    :param funny:number of funny votes sent by the user
    :param cool:number of cool votes sent by the user
    :param fans:number of fans the user has
    :param average_stars: average rating of all reviews
    :return: cleaned user dataset
    """
    return df.withColumn(review_count, f.col(review_count).cast('Int')) \
        .withColumn(useful, f.col(useful).cast('Int')) \
        .withColumn(funny, f.col(funny).cast('Int')) \
        .withColumn(cool, f.col(cool).cast('Int')) \
        .withColumn(fans, f.col(fans).cast('Int')) \
        .withColumn(average_stars, f.col(average_stars).cast('Float')) \
        .select(f.col(user_id), f.col(name), f.col(review_count), f.col(useful),
                f.col(funny), f.col(cool), f.col(fans),
                f.col(average_stars)) \
        .dropna(subset=[user_id])
