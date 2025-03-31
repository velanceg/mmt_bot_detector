from pyspark.sql.functions import col, count, window
from pyspark.sql import DataFrame


def detect_bots(events_df: DataFrame, threshold: int) -> DataFrame:
    """
    Identify bot users based on activity threshold.
    """
    return (
        events_df
        .groupBy("user_id", window(col("timestamp"), "1 hour", "5 minutes"))
        .agg(count("*").alias("activity_count"))
        .filter(col("activity_count") > threshold)
        .select("user_id")
    )