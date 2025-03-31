import configparser

config = configparser.ConfigParser()
config.read("config.ini")

# Redis Configs
REDIS_HOST = config.get("redis", "host", fallback="localhost")
REDIS_PORT = config.getint("redis", "port", fallback=6379)

# Kafka Configs
KAFKA_BROKER = config.get("kafka", "broker", fallback="localhost:9092")
KAFKA_INPUT_TOPIC = config.get("kafka", "input_topic", fallback="user_events")
KAFKA_BOT_OUTPUT_TOPIC = config.get("kafka", "bot_events_output")

# HDFS Config
HDFS_PATH = config.get("hdfs", "hdfs://nameservice/data/bot_users", fallback="/data/bot_users")

# Bot Detection Threshold
ACTIVITY_THRESHOLD = config.getint("bot", "activity_threshold", fallback=100)