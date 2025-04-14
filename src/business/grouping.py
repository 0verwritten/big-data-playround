from ..base import BaseClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour, dayofweek, avg, count, sum, round, desc

class Grouping(BaseClass):
    pass

    def trips_by_day_of_week(self, df: DataFrame) -> DataFrame:
        """
        Бізнес-питання Vi: Яка кількість поїздок та середня вартість за днями тижня?
        """
        self.logger.info(f"Виконання бізнес-питання: Поїздки за днями тижня")

        result = df.groupBy(
            dayofweek("pickup_datetime").alias("day_of_week")
        ).agg(
            count("*").alias("trip_count"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("total_amount"), 2).alias("avg_total"),
            round(avg("trip_distance"), 2).alias("avg_distance")
        ).orderBy("day_of_week")

        return result