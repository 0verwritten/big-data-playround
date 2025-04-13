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
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

    @property
    def logger(self):
        return logging.getLogger(__name__)