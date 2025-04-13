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