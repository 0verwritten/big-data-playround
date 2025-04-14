from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr, count

class Joins(BaseClass):
    pass

    def distance_fare_correlation(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Яке співвідношення між відстанню поїздки та вартістю?
        """
        self.logger.info(f"Виконання бізнес-питання: Співвідношення відстані та вартості")
        
        result = df.select(
            "medallion",
            "pickup_datetime",
            "trip_distance",
            "fare_amount",
            round(col("fare_amount") / col("trip_distance"), 2).alias("cost_per_mile")
        ).filter(
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0)
        ).orderBy(
            desc("trip_distance")
        )
        
        return result

    def trip_time_fare_analysis(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Як співвідносяться тривалість поїздки та її вартість?
        """
        self.logger.info(f"Виконання бізнес-питання: Аналіз часу подорожі і вартості")
        
        result = df.select(
            "medallion",
            "hack_license",
            "pickup_datetime",
            "trip_distance",
            "trip_time_in_secs",
            "fare_amount",
            "total_amount"
        ).filter(
            (col("trip_time_in_secs") > 0) &
            (col("fare_amount") > 0)
        ).withColumn(
            "minutes", round(col("trip_time_in_secs") / 60, 1)
        ).withColumn(
            "cost_per_minute", round(col("fare_amount") / (col("trip_time_in_secs") / 60), 2)
        ).withColumn(
            "cost_per_mile", round(col("fare_amount") / col("trip_distance"), 2)
        ).orderBy(
            desc("cost_per_minute")
        )
        
        return result

    def trip_time_tip_correlation(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Який зв'язок між часом поїздки та розміром чайових?
        """
        self.logger.info(f"Виконання бізнес-питання: Зв'язок між часом поїздки та чайовими")
        
        result = df.select(
            "hack_license",
            "pickup_datetime",
            "trip_time_in_secs",
            "trip_distance",
            "fare_amount",
            "tip_amount"
        ).filter(
            (col("trip_time_in_secs") > 0) &
            (col("tip_amount") >= 0) &
            (col("fare_amount") > 0)
        ).withColumn(
            "minutes", round(col("trip_time_in_secs") / 60, 1)
        ).withColumn(
            "tip_percentage", round((col("tip_amount") / col("fare_amount")) * 100, 2)
        ).withColumn(
            "tip_per_minute", round(col("tip_amount") / (col("trip_time_in_secs") / 60), 3)
        ).withColumn(
            "time_bucket", 
            when(col("minutes") < 10, "0-10 min")
            .when(col("minutes") < 20, "10-20 min")
            .when(col("minutes") < 30, "20-30 min")
            .when(col("minutes") < 45, "30-45 min")
            .when(col("minutes") < 60, "45-60 min")
            .otherwise("60+ min")
        ).groupBy(
            "time_bucket"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("tip_amount"), 2).alias("avg_tip"),
            round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            round(avg("tip_per_minute"), 3).alias("avg_tip_per_minute")
        ).orderBy(
            "time_bucket"
        )
        
        return result
