from pyspark.sql import SparkSession
import logging

class BaseClass:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.spark = SparkSession.builder \
            .appName("NYC Taxi Data Analysis") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "4g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

    @getattr
    def logging():
        return logging.getLogger(__name__)