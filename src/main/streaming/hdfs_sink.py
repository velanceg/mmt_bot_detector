from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, hdfs_path: str):
    """
    Write bot user IDs to HDFS in append mode.
    """
    query = df.writeStream \
        .format("csv") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", hdfs_path + "/checkpoint") \
        .outputMode("append") \
        .start()

    return query
