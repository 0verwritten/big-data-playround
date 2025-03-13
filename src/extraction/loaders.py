from ..base import BaseClass
from ..transformation.cleaning import Cleaning
from .schemas import Schemas


class Loaders(BaseClass, Cleaning, Schemas):
    def load_trip_data(self):
        for i in range(1, 13):
            path = f"dataset/trip_data/trip_data_{i}.csv"
            df = self.spark.read.csv(
                path, header=True, schema=self.trip_data_schema, inferSchema=True
            )

            output_path = f"dataset/trip_data_parquet"
            df.write.mode("append").parquet(output_path)

        df = self.spark.read.parquet(output_path).schema(self.trip_data_schema)
        cleaned_df = self.clean_trip_data(df)

        cleaned_output_path = f"dataset/trip_data_cleaned_parquet"
        cleaned_df.write.mode("overwrite").parquet(cleaned_output_path)
