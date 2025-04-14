from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rank, hour, dayofweek, date_format, to_date, when, avg, expr, count, dense_rank, year, month
from pyspark.sql.window import Window

class Windows(BaseClass):
    pass

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
