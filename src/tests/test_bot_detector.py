import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.main.streaming.bot_detector import detect_bots


class BotDetectorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_bot_detection(self):
        test_data = [
            ("user1", "search", "2025-03-31 10:01:00"),
            ("user1", "click", "2025-03-31 10:02:00"),
            ("user1", "booking", "2025-03-31 10:03:00"),
            ("user2", "search", "2025-03-31 10:04:00")
        ]

        df = self.spark.createDataFrame(test_data, ["user_id", "activity_name", "timestamp"])
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        result = detect_bots(df, threshold=2)
        self.assertEqual(result.count(), 1)  # user1 should be flagged as bot


if __name__ == "__main__":
    unittest.main()