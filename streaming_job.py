from pyspark.sql import SparkSession
from src.main.config.config_loader import REDIS_HOST, REDIS_PORT, ACTIVITY_THRESHOLD, HDFS_PATH, KAFKA_BROKER, KAFKA_INPUT_TOPIC, KAFKA_BOT_OUTPUT_TOPIC
from src.main.streaming.bot_detector import detect_bots
from src.main.streaming.redis_sink import write_to_redis
from src.main.streaming.hdfs_sink import write_to_hdfs
from src.main.streaming.kafka_sink import write_to_kafka

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BotUserDetection") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Read events from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize Kafka messages
events_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .selectExpr(
        "get_json_object(value, '$.user_id') AS user_id",
        "get_json_object(value, '$.activity_name') AS activity_name",
        "get_json_object(value, '$.timestamp') AS timestamp"
    )

# Detect Bot Users
bot_users_df = detect_bots(events_df, ACTIVITY_THRESHOLD)

# Write bot user IDs to Redis and HDFS
redis_query = write_to_redis(bot_users_df, REDIS_HOST, REDIS_PORT)
hdfs_query = write_to_hdfs(bot_users_df, HDFS_PATH)

# Filter bot user events from raw stream
bot_user_events_df = events_df.join(bot_users_df, "user_id")

# Write filtered bot user events to Kafka
kafka_query = write_to_kafka(bot_user_events_df, KAFKA_BOT_OUTPUT_TOPIC, KAFKA_BROKER)

# Wait for termination
spark.streams.awaitAnyTermination()
