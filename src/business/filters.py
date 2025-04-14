from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr

class Filters(BaseClass):
    pass


    def get_weekend_trips(self, df: DataFrame, limit: int = 100) -> DataFrame:
        """
        Бізнес-питання N: Які поїздки відбувались у вихідні дні (субота та неділя)?
        """
        self.logger.info(f"Виконання бізнес-питання: Поїздки у вихідні дні")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "dropoff_datetime", 
            "trip_distance",
            "passenger_count", 
            "fare_amount",
            "total_amount",
            dayofweek("pickup_datetime").alias("day_of_week")
        ).filter(
            (dayofweek("pickup_datetime").isin([1, 7]))  # 1 = неділя, 7 = субота в Spark SQL
        ).orderBy(
            "pickup_datetime"
        ).limit(limit)
        
        return result

    def get_rush_hour_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання N: Які поїздки відбувались у години пік (7-10 ранку та 16-19 вечора)?
        """
        self.logger.info(f"Виконання бізнес-питання: Поїздки в години пік")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "dropoff_datetime", 
            "trip_distance",
            "passenger_count", 
            "fare_amount",
            "total_amount",
            hour("pickup_datetime").alias("hour_of_day")
        ).filter(
            (hour("pickup_datetime").between(7, 10) | hour("pickup_datetime").between(16, 19))
        ).groupBy(
            hour("pickup_datetime").alias("hour")
        ).count().orderBy("hour")
        
        return result

    def get_short_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання N: Які характеристики мають короткі поїздки (менше 1 милі)?
        """
        self.logger.info("Виконання бізнес-питання: Короткі поїздки")
        
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
            (col("trip_distance") > 0) & (col("trip_distance") < 1)
        ).withColumn(
            "cost_per_mile", col("fare_amount") / col("trip_distance")
        ).withColumn(
            "avg_speed_mph", (col("trip_distance") / col("trip_time_in_secs")) * 3600
        ).orderBy(
            desc("cost_per_mile")
        )
        
        return result