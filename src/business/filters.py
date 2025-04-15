from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr

class Filters(BaseClass):
    def get_longest_trips(self, df: DataFrame, limit: int = 100) -> DataFrame:
        """
        Бізнес-питання Vl: Які поїздки були найдовшими за відстанню?
        """
        self.logger.info(f"Виконання бізнес-питання: Найдовші поїздки (top {limit})")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "dropoff_datetime", 
            "trip_distance",
            "passenger_count", 
            "total_amount"
        ).filter(
            col("trip_distance") > 0
        ).orderBy(
            desc("trip_distance")
        ).limit(limit)
        
        return result

    def get_most_expensive_trips(self, df: DataFrame, limit: int = 100) -> DataFrame:
        """
        Бізнес-питання Vl: Які поїздки були найдорожчими?
        """
        self.logger.info(f"Виконання бізнес-питання: Найдорожчі поїздки (top {limit})")
        
        result = df.select(
            "medallion", 
            "hack_license", 
            "pickup_datetime", 
            "dropoff_datetime", 
            "trip_distance",
            "passenger_count", 
            "fare_amount",
            "tip_amount",
            "total_amount"
        ).filter(
            (col("total_amount") > 0) &
            (col("fare_amount") > 0)
        ).orderBy(
            desc("total_amount")
        ).limit(limit)
        
        return result

    def get_night_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vl: Які поїздки відбувались у нічний час (між опівночі та 5 ранку)?
        """
        self.logger.info("Виконання бізнес-питання: Поїздки в нічний час")
        
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
            (hour("pickup_datetime") >= 0) & (hour("pickup_datetime") < 5)
        ).groupBy(
            hour("pickup_datetime").alias("hour")
        ).count().orderBy("hour")
        
        return result
    
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