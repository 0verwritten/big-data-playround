from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from ..base import BaseClass

class Cleaning(BaseClass):
    def clean_trip_data(self, trip_df: DataFrame) -> DataFrame:
        self.logger.info("Очищення даних поїздок...")

        cleaned_df = trip_df.filter(
            (col("pickup_datetime").isNotNull())
        )

        cleaned_df = cleaned_df.filter(
            (col("passenger_count") >= 1)
        )

        cleaned_df = cleaned_df.filter(
            (col("trip_distance") > 0)
        )

        cleaned_df = cleaned_df.filter(
            (col("pickup_longitude") != 0) &
            (col("pickup_latitude") != 0) &
            (col("dropoff_longitude") != 0) &
            (col("dropoff_latitude") != 0)
        )

        original_count = trip_df.count()
        cleaned_count = cleaned_df.count()
        removed_count = original_count - cleaned_count

        self.logger.info(f"Видалено {removed_count} записів ({removed_count/original_count*100:.2f}%) з даних поїздок")

        return cleaned_df
