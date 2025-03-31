from pyspark.sql import DataFrame


def write_to_kafka(df: DataFrame, kafka_broker: str, topic: str):
    """
    Write bot user events to Kafka.
    """
    query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", topic) \
        .option("checkpointLocation", "/tmp/kafka-checkpoint") \
        .start()

    return query
