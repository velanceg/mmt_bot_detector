from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count

def detect_bots(df: DataFrame, threshold: int):
    """
    Detect bot users based on activity count threshold.
    Returns a DataFrame with columns: [user_id]
    """
    bot_users_df = (
        df.groupBy("user_id", window("timestamp", "1 hour", "5 minutes"))
        .agg(count("activity_name").alias("activity_count"))
        .filter(f"activity_count > {threshold}")
        .select("user_id")
        .distinct()
    )
    return bot_users_df