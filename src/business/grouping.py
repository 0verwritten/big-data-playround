from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour, dayofweek, avg, count, sum, round, desc

class Grouping(BaseClass):
    def hourly_trip_fare(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання N: Яка середня вартість поїздки за годинами доби?
        """
        self.logger.info(f"Виконання бізнес-питання: Середня вартість поїздки за годинами")

        result = df.groupBy(
            hour("pickup_datetime").alias("hour")
        ).agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("trip_distance"), 2).alias("avg_distance")
        ).orderBy("hour")

        return result