from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rank, hour, dayofweek, date_format, to_date, when, avg, expr, count
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