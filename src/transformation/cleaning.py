from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour, dayofweek, when
from ..base import BaseClass

class Cleaning(BaseClass):
    def clean_trip_data(self, trip_df: DataFrame) -> DataFrame:
        self.logger.info("Очищення даних поїздок...")

        cleaned_df = trip_df.filter(
            (col("medallion").isNotNull()) &
            (col("hack_license").isNotNull()) &
            (col("vendor_id").isNotNull()) &
            (col("pickup_datetime").isNotNull())
        )

        cleaned_df = cleaned_df.filter(
            (col("passenger_count") >= 1) &
            (col("passenger_count") <= 9)
        )

        cleaned_df = cleaned_df.filter(
            (col("trip_distance") > 0) &
            (col("trip_distance") < 100) # roughly
        )

        cleaned_df = cleaned_df.filter(
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0) &
            (col("dropoff_longitude") != 0) &
            (col("dropoff_latitude") != 0)
        )

        cleaned_df = cleaned_df.filter((col("trip_time_in_secs") > 0) & (col('trip_distance') > 0))

        cleaned_df = cleaned_df.withColumn("pickup_year", year(col("pickup_datetime")))
        cleaned_df = cleaned_df.withColumn("pickup_month", month(col("pickup_datetime")))
        cleaned_df = cleaned_df.withColumn("pickup_day", dayofmonth(col("pickup_datetime")))
        cleaned_df = cleaned_df.withColumn("pickup_hour", hour(col("pickup_datetime")))
        cleaned_df = cleaned_df.withColumn("pickup_dayofweek", dayofweek(col("pickup_datetime")))

        original_count = trip_df.count()
        cleaned_count = cleaned_df.count()
        removed_count = original_count - cleaned_count

        self.logger.info(f"Видалено {removed_count} записів ({removed_count/original_count*100:.2f}%) з даних поїздок")

        return cleaned_df


    def clean_fare_data(self, fare_df: DataFrame) -> DataFrame:
        self.logger.info("Очищення даних оплати...")

        cleaned_df = fare_df.filter(
            (col("medallion").isNotNull()) &
            (col("hack_license").isNotNull()) &
            (col("vendor_id").isNotNull()) &
            (col("pickup_datetime").isNotNull())
        )

        cleaned_df = fare_df.filter(
            (col("fare_amount") >= 2.5)
        )

        cleaned_df = cleaned_df.filter(
            (col("total_amount") >= 2.5)
        )

        cleaned_df = cleaned_df.withColumn(
            "payment_type",
            when(col("payment_type").isNull(), "UNKNOWN")
            .otherwise(col("payment_type"))
        )

        original_count = fare_df.count()
        cleaned_count = cleaned_df.count()
        removed_count = original_count - cleaned_count

        self.logger.info(f"Видалено {removed_count} записів ({removed_count/original_count*100:.2f}%) з даних оплати")

        return cleaned_df
