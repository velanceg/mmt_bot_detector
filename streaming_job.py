from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from src.main.streaming.bot_detector import detect_bots
from src.main.streaming.redis_sink import write_to_redis
from src.main.streaming.hdfs_sink import write_to_hdfs
from src.main.config.config_loader import REDIS_HOST, REDIS_PORT, ACTIVITY_THRESHOLD, HDFS_PATH


def main():
    spark = SparkSession.builder.appName("BotUserDetector").getOrCreate()

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("activity_name", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    raw_events_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("hdfs://path_to_events")
    )

    bot_users_df = detect_bots(raw_events_df, ACTIVITY_THRESHOLD)

    write_to_redis(bot_users_df, REDIS_HOST, REDIS_PORT)
    write_to_hdfs(bot_users_df, HDFS_PATH)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()