from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr

class Filters(BaseClass):
    pass


    def get_high_tip_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання K: Які поїздки мають високі чайові (більше 20% від вартості поїздки)?
        """
        self.logger.info("Виконання бізнес-питання: Поїздки з високими чайовими")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "trip_distance",
            "passenger_count", 
            "fare_amount",
            "tip_amount",
            "total_amount"
        ).filter(
            (col("fare_amount") > 0) & (col("tip_amount") > 0)
        ).withColumn(
            "tip_percentage", (col("tip_amount") / col("fare_amount")) * 100
        ).filter(
            col("tip_percentage") > 20
        ).orderBy(
            desc("tip_percentage")
        )
        
        return result

    def get_high_speed_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання K: Які поїздки мають високу швидкість?
        """
        self.logger.info("Виконання бізнес-питання: Поїздки з високою швидкістю")
        
        avg_speed = df.filter(
            (col("trip_distance") > 0) & (col("trip_time_in_secs") > 0)
        ).withColumn(
            "speed_mph", (col("trip_distance") / col("trip_time_in_secs")) * 3600
        ).agg(
            avg("speed_mph").alias("avg_speed")
        ).collect()[0]["avg_speed"]
        
        result = df.filter(
            (col("trip_distance") > 0) & (col("trip_time_in_secs") > 0)
        ).withColumn(
            "speed_mph", (col("trip_distance") / col("trip_time_in_secs")) * 3600
        ).filter(
            (col("speed_mph") > avg_speed) & (col("speed_mph") < 100)
        ).orderBy(
            desc("speed_mph")
        )
        
        return result

    def get_many_passengers_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання K: Які характеристики поїздок з великою кількістю пасажирів (4 і більше)?
        """
        self.logger.info("Виконання бізнес-питання: Поїздки з великою кількістю пасажирів")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "trip_distance",
            "trip_time_in_secs",
            "passenger_count", 
            "fare_amount",
            "tip_amount",
            "total_amount"
        ).filter(
            col("passenger_count") >= 4
        ).withColumn(
            "fare_per_passenger", col("fare_amount") / col("passenger_count")
        ).withColumn(
            "tip_per_passenger", col("tip_amount") / col("passenger_count")
        ).orderBy(
            "pickup_datetime"
        )
        
        return result