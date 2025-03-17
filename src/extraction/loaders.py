from ..base import BaseClass
from ..transformation.cleaning import Cleaning
from .schemas import Schemas


class Loaders(Schemas, Cleaning):
    def load_trip_data(self):
        output_path = f"dataset/trip_data_parquet"
        # for i in range(1, 13):
        #     path = f"dataset/trip_data/trip_data_{i}.csv"
        #     df = self.spark.read.csv(
        #         path, header=True, schema=self.trip_data_schema, inferSchema=False, delimiter=", "
        #     )

        #     df.write.mode("append").parquet(output_path)

        df = self.spark.read.parquet(output_path)
        cleaned_df = self.clean_trip_data(df)

        cleaned_output_path = f"dataset/trip_data_cleaned_parquet"
        cleaned_df.write.mode("overwrite").parquet(cleaned_output_path)

    def load_fare_data(self):
        output_path = f"dataset/trip_fare_parquet"
        for i in range(1, 13):
            path = f"dataset/trip_fare/trip_fare_{i}.csv"
            df = self.spark.read.csv(
                path, header=True, schema=self.fare_data_schema, inferSchema=False, delimiter=", "
            )

            df.write.mode("append").parquet(output_path)

        df = self.spark.read.parquet(output_path)
        cleaned_df = self.clean_fare_data(df)

        cleaned_output_path = f"dataset/trip_fare_cleaned_parquet"
        cleaned_df.write.mode("overwrite").parquet(cleaned_output_path)
