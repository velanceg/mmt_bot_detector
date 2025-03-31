from pyspark.sql.streaming import ForeachWriter
from pyspark.sql import DataFrame
import redis


class RedisSink(ForeachWriter):
    def __init__(self, redis_host: str, redis_port: int):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.client = None

    def open(self, partition_id: int, epoch_id: int):
        """Initialize Redis connection for each partition."""
        self.client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        return True

    def process(self, row):
        """Write user_id to Redis as a bot user."""
        if self.client:
            self.client.sadd("bot_users", row["user_id"])

    def close(self, error):
        """Close Redis connection."""
        if self.client:
            self.client.close()


def write_to_redis(df: DataFrame, redis_host: str, redis_port: int):
    """Write bot user IDs to Redis using ForeachWriter."""
    query = df.writeStream.foreach(RedisSink(redis_host, redis_port)).start()
    return query