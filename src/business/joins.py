from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr, count

class Joins(BaseClass):

    def payment_type_statistics(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vl: Які статистичні дані по різних типах оплати?
        """
        self.logger.info(f"Виконання бізнес-питання: Статистика по типах оплати")
        
        result = df.groupBy("payment_type").agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("tip_amount"), 2).alias("avg_tip"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(sum("fare_amount"), 2).alias("total_fare_amount"),
            round(sum("tip_amount"), 2).alias("total_tip_amount")
        ).withColumn(
            "avg_tip_percentage", round((col("avg_tip") / col("avg_fare")) * 100, 2)
        ).orderBy(
            desc("trip_count")
        )
        
        return result

    def passenger_count_fare_analysis(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vl: Як впливає кількість пасажирів на вартість поїздки?
        """
        self.logger.info(f"Виконання бізнес-питання: Вплив кількості пасажирів на вартість")
        
        result = df.filter(
            (col("passenger_count") > 0) &
            (col("passenger_count") <= 9) &
            (col("fare_amount") > 0)
        ).withColumn(
            "fare_per_passenger", round(col("fare_amount") / col("passenger_count"), 2)
        ).groupBy(
            "passenger_count"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("fare_per_passenger"), 2).alias("avg_fare_per_passenger"),
            round(avg("trip_distance"), 2).alias("avg_distance")
        ).orderBy(
            "passenger_count"
        )
        
        return result