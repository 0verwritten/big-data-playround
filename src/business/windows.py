from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rank, hour, dayofweek, date_format, to_date, when, avg, expr, count, dense_rank, year, month
from pyspark.sql.window import Window

class Windows(BaseClass):
    def popular_routes_ranking(self, df: DataFrame, limit: int = 100) -> DataFrame:
        """
        Бізнес-питання Vl: Рейтинг найпопулярніших маршрутів таксі.
        """
        self.logger.info(f"Виконання бізнес-питання: Рейтинг найпопулярніших маршрутів")
        
        routes_df = df.select(
            round(col("pickup_longitude"), 3).alias("pickup_lng"),
            round(col("pickup_latitude"), 3).alias("pickup_lat"),
            round(col("dropoff_longitude"), 3).alias("dropoff_lng"),
            round(col("dropoff_latitude"), 3).alias("dropoff_lat")
        ).filter(
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0) &
            (col("dropoff_longitude") != 0) &
            (col("dropoff_latitude") != 0)
        )
        
        routes_count = routes_df.groupBy(
            "pickup_lng", "pickup_lat", "dropoff_lng", "dropoff_lat"
        ).count().orderBy(col("count").desc()).limit(limit)
        
        windowSpec = Window.orderBy(desc("count"))
        result = routes_count.withColumn("rank", rank().over(windowSpec))
        
        return result

    def popular_pickup_areas_ranking(self, df: DataFrame, limit: int = 50) -> DataFrame:
        """
        Бізнес-питання N: Рейтинг найпопулярніших районів для початку поїздки.
        """
        self.logger.info(f"Виконання бізнес-питання: Рейтинг районів за кількістю поїздок")
        
        areas_df = df.select(
            round(col("pickup_longitude"), 2).alias("area_lng"),
            round(col("pickup_latitude"), 2).alias("area_lat")
        ).filter(
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0)
        )
        
        areas_count = areas_df.groupBy(
            "area_lng", "area_lat"
        ).count().orderBy(desc("count"))
        
        windowSpec = Window.orderBy(desc("count"))
        result = areas_count.withColumn(
            "rank", dense_rank().over(windowSpec)
        ).limit(limit)
        
        return result

    def monthly_trips_comparison(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання N: Щомісячна динаміка кількості поїздок відносно середньої.
        """
        self.logger.info(f"Виконання бізнес-питання: Щомісячна динаміка кількості поїздок")
        
        monthly_counts = df.withColumn(
            "year", year("pickup_datetime")
        ).withColumn(
            "month", month("pickup_datetime")
        ).groupBy("year", "month").count()
        
        windowSpec = Window.partitionBy("month")
        
        result = monthly_counts.withColumn(
            "avg_monthly_count", avg("count").over(windowSpec)
        ).withColumn(
            "diff_from_avg", col("count") - col("avg_monthly_count")
        ).withColumn(
            "percent_of_avg", round((col("count") / col("avg_monthly_count")) * 100, 2)
        ).orderBy("year", "month")
        
        return result

    def driver_speed_ranking(self, df: DataFrame, min_trips: int = 100) -> DataFrame:
        """
        Бізнес-питання K: Рейтинг водіїв за середньою швидкістю поїздок.

        """
        self.logger.info(f"Виконання бізнес-питання: Рейтинг водіїв за швидкістю")
        
        speed_df = df.filter(
            (col("trip_time_in_secs") > 0) &
            (col("trip_distance") > 0)
        ).withColumn(
            "speed_mph", round((col("trip_distance") / col("trip_time_in_secs")) * 3600, 2)
        ).filter(
            col("speed_mph").between(1, 100)
        )
        
        driver_speeds = speed_df.groupBy("hack_license").agg(
            count("*").alias("trip_count"),
            round(avg("speed_mph"), 2).alias("avg_speed_mph"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(avg("trip_time_in_secs") / 60, 2).alias("avg_minutes")
        ).filter(
            col("trip_count") >= min_trips
        )
        
        windowSpec = Window.orderBy(desc("avg_speed_mph"))
        
        result = driver_speeds.withColumn(
            "rank", dense_rank().over(windowSpec)
        ).orderBy("rank")
        
        return result
