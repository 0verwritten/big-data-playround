from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, hour, dayofweek, date_format, to_date, when, avg, expr, count, round, sum

class Joins(BaseClass):

    def payment_type_statistics(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vl: Які статистичні дані по різних типах оплати?
        """
        self.logger.info(f"Виконання бізнес-питання: Статистика по типах оплати")
        
        result = df.groupBy("payment_type").agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("tip_amount"), 2).alias("avg_tip"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(sum("fare_amount"), 2).alias("total_fare_amount"),
            round(sum("tip_amount"), 2).alias("total_tip_amount")
        ).withColumn(
            "avg_tip_percentage", round((col("avg_tip") / col("avg_fare")) * 100, 2)
        ).orderBy(
            desc("trip_count")
        )
        
        return result

    def passenger_count_fare_analysis(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vl: Як впливає кількість пасажирів на вартість поїздки?
        """
        self.logger.info(f"Виконання бізнес-питання: Вплив кількості пасажирів на вартість")
        
        result = df.filter(
            (col("passenger_count") > 0) &
            (col("passenger_count") <= 9) &
            (col("fare_amount") > 0)
        ).withColumn(
            "fare_per_passenger", round(col("fare_amount") / col("passenger_count"), 2)
        ).groupBy(
            "passenger_count"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("fare_per_passenger"), 2).alias("avg_fare_per_passenger"),
            round(avg("trip_distance"), 2).alias("avg_distance")
        ).orderBy(
            "passenger_count"
        )
        
        return result
    

    def day_of_week_tip_analysis(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання K: Як впливає день тижня на розмір чайових?
        """
        self.logger.info(f"Виконання бізнес-питання: Вплив дня тижня на чайові")
        
        days_map = {
            1: "Неділя",
            2: "Понеділок",
            3: "Вівторок",
            4: "Середа",
            5: "Четвер",
            6: "П\\'ятниця",
            7: "Субота"
        }
        
        df_with_day_name = df.withColumn(
            "day_of_week", dayofweek("pickup_datetime")
        ).withColumn(
            "day_name", expr(f"CASE day_of_week " + 
                            " ".join([f"WHEN {k} THEN '{v}'" for k, v in days_map.items()]) +
                            " END")
        )
        
        result = df_with_day_name.filter(
            (col("fare_amount") > 0) &
            (col("tip_amount") >= 0)
        ).withColumn(
            "tip_percentage", round((col("tip_amount") / col("fare_amount")) * 100, 2)
        ).withColumn(
            "has_tip", col("tip_amount") > 0
        ).groupBy(
            "day_of_week", "day_name"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("tip_amount"), 2).alias("avg_tip"),
            round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            round((sum(when(col("has_tip"), 1).otherwise(0)) / count("*")) * 100, 2).alias("percentage_with_tip")
        ).orderBy(
            "day_of_week"
        )
        
        return result


    def highest_cost_per_mile_routes(self, df: DataFrame, min_trips: int = 10) -> DataFrame:
        """
        Бізнес-питання K: Які маршрути мають найвищу вартість за милю?
        """
        self.logger.info(f"Виконання бізнес-питання: Маршрути з найвищою вартістю за милю")
        
        routes_df = df.select(
            round(col("pickup_longitude"), 3).alias("pickup_lng"),
            round(col("pickup_latitude"), 3).alias("pickup_lat"),
            round(col("dropoff_longitude"), 3).alias("dropoff_lng"),
            round(col("dropoff_latitude"), 3).alias("dropoff_lat"),
            col("fare_amount"),
            col("trip_distance")
        ).filter(
            (col("trip_distance") > 1) &
            (col("fare_amount") > 0) &
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0) &
            (col("dropoff_longitude") != 0) &
            (col("dropoff_latitude") != 0)
        ).withColumn(
            "cost_per_mile", round(col("fare_amount") / col("trip_distance"), 2)
        )
        
        result = routes_df.groupBy(
            "pickup_lng", "pickup_lat", "dropoff_lng", "dropoff_lat"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("cost_per_mile"), 2).alias("avg_cost_per_mile"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("trip_distance"), 2).alias("avg_distance")
        ).filter(
            col("trip_count") >= min_trips
        ).orderBy(
            desc("avg_cost_per_mile")
        ).limit(100)
        
        return result

    def area_trip_duration_analysis(self, df: DataFrame, min_trips: int = 50) -> DataFrame:
        """
        Бізнес-питання K: Як залежить тривалість поїздки від району міста?
        
        Args:
            spark: SparkSession
            df: DataFrame з даними
            min_trips: мінімальна кількість поїздок для району
            
        Returns:
            DataFrame: результат запиту
        """
        self.logger.info(f"Виконання бізнес-питання: Тривалість поїздки залежно від району")
        
        area_df = df.select(
            round(col("pickup_longitude"), 2).alias("area_lng"),
            round(col("pickup_latitude"), 2).alias("area_lat"),
            col("trip_time_in_secs"),
            col("trip_distance")
        ).filter(
            (col("trip_time_in_secs") > 0) &
            (col("trip_distance") > 0) &
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0)
        ).withColumn(
            "minutes", round(col("trip_time_in_secs") / 60, 1)
        ).withColumn(
            "speed_mph", round((col("trip_distance") / col("trip_time_in_secs")) * 3600, 2)
        )
        
        result = area_df.groupBy(
            "area_lng", "area_lat"
        ).agg(
            count("*").alias("trip_count"),
            round(avg("minutes"), 2).alias("avg_minutes"),
            round(avg("trip_distance"), 2).alias("avg_distance"),
            round(avg("speed_mph"), 2).alias("avg_speed_mph"),
            round(avg(col("trip_distance") / col("minutes")), 2).alias("avg_miles_per_minute")
        ).filter(
            col("trip_count") >= min_trips
        ).orderBy(
            desc("avg_speed_mph")
        ).limit(100)
        
        return result

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
