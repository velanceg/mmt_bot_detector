from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, hdfs_path: str):
    """
    Write bot user data to HDFS in Parquet format.
    """
    df.writeStream.format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", f"{hdfs_path}/_checkpoint") \
        .start()