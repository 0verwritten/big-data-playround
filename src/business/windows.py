from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rank, hour, dayofweek, date_format, to_date, when, avg, expr, count, dense_rank, year, month, dayofmonth
from pyspark.sql.window import Window

class Windows(BaseClass):
    pass

    def fare_comparison_by_day(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Порівняння вартості поїздки з середньою за день.
        """
        self.logger.info(f"Виконання бізнес-питання: Порівняння вартості з середньою")
        
        df_with_date = df.withColumn("trip_date", to_date(col("pickup_datetime")))
        
        windowSpec = Window.partitionBy("trip_date")
        
        result = df_with_date.withColumn(
            "avg_daily_fare", avg("fare_amount").over(windowSpec)
        ).withColumn(
            "fare_vs_avg", round(col("fare_amount") - col("avg_daily_fare"), 2)
        ).withColumn(
            "fare_percent_of_avg", round((col("fare_amount") / col("avg_daily_fare")) * 100, 2)
        ).select(
            "medallion",
            "pickup_datetime",
            "trip_date",
            "fare_amount",
            "avg_daily_fare",
            "fare_vs_avg",
            "fare_percent_of_avg"
        ).orderBy("trip_date", "pickup_datetime")
        
        return result

    def speed_comparison_by_day(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Порівняння швидкості поїздки з середньою за день.
        """
        self.logger.info(f"Виконання бізнес-питання: Порівняння швидкості з середньою")
        
        speed_df = df.filter(
            (col("trip_time_in_secs") > 0) &
            (col("trip_distance") > 0)
        ).withColumn(
            "speed_mph", round((col("trip_distance") / col("trip_time_in_secs")) * 3600, 2)
        ).withColumn(
            "trip_date", to_date(col("pickup_datetime"))
        ).filter(
            col("speed_mph").between(1, 100)  # фільтрація нереалістичних значень
        )
        
        windowSpec = Window.partitionBy("trip_date")
        
        result = speed_df.withColumn(
            "avg_daily_speed", avg("speed_mph").over(windowSpec)
        ).withColumn(
            "speed_vs_avg", round(col("speed_mph") - col("avg_daily_speed"), 2)
        ).withColumn(
            "speed_percent_of_avg", round((col("speed_mph") / col("avg_daily_speed")) * 100, 2)
        ).select(
            "medallion",
            "hack_license",
            "pickup_datetime",
            "trip_date",
            "trip_distance",
            "trip_time_in_secs",
            "speed_mph",
            "avg_daily_speed",
            "speed_vs_avg",
            "speed_percent_of_avg"
        ).orderBy(
            desc("speed_percent_of_avg")
        )
        
        return result

    def cumulative_monthly_trips(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Кумулятивна сума поїздок протягом місяця.
        """
        self.logger.info(f"Виконання бізнес-питання: Кумулятивна сума поїздок протягом місяця")
        
        daily_counts = df.withColumn(
            "trip_date", to_date(col("pickup_datetime"))
        ).withColumn(
            "day", dayofmonth("pickup_datetime")
        ).withColumn(
            "month", month("pickup_datetime")
        ).withColumn(
            "year", year("pickup_datetime")
        ).groupBy("trip_date", "day", "month", "year").count()
        
        windowSpec = Window.partitionBy("month", "year").orderBy("day")
        
        result = daily_counts.withColumn(
            "cumulative_trips", sum("count").over(windowSpec)
        ).withColumn(
            "total_monthly_trips", sum("count").over(Window.partitionBy("month", "year"))
        ).withColumn(
            "percent_of_month", round((col("cumulative_trips") / col("total_monthly_trips")) * 100, 2)
        ).select(
            "trip_date",
            "day",
            "month",
            "year",
            "count",
            "cumulative_trips",
            "total_monthly_trips",
            "percent_of_month"
        ).orderBy("year", "month", "day")
        
        return result