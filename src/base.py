from pyspark.sql import SparkSession
import logging

class BaseClass:
    spark: SparkSession

    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        builder: SparkSession.Builder = SparkSession.builder
        self.spark = builder \
            .appName("NYC Taxi Data Analysis") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

    @property
    def logger(self):
        return logging.getLogger(__name__)