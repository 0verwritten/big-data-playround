from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rank, hour, dayofweek, date_format, to_date, when, avg, expr, count, dense_rank, year, month
from pyspark.sql.window import Window

class Windows(BaseClass):
    pass
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