import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from src.main.streaming.bot_detector import detect_bots


class BotDetectorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_detect_bots(self):
        test_data = [
            ("user1", "click", "2025-03-31 10:00:00"),
            ("user1", "search", "2025-03-31 10:01:00"),
            ("user1", "page_view", "2025-03-31 10:02:00"),
            ("user1", "booking", "2025-03-31 10:03:00"),
            ("user2", "click", "2025-03-31 10:00:00"),
        ]

        df = self.spark.createDataFrame(test_data, ["user_id", "activity_name", "timestamp"])
        df = df.withColumn("timestamp", to_timestamp(df.timestamp))

        bot_users = detect_bots(df, threshold=3).collect()
        assert len(bot_users) == 1
        assert bot_users[0]["user_id"] == "user1"


if __name__ == "__main__":
    unittest.main()