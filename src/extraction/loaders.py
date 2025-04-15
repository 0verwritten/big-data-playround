from os.path import exists
from ..base import BaseClass
from ..transformation.cleaning import Cleaning
from .schemas import Schemas
from pyspark.sql.functions import broadcast, month, dayofmonth

class Loaders(Schemas, Cleaning):
    __trip_output_path = f"dataset/trip_data_parquet"
    __trip_cleaned_output_path = f"dataset/trip_data_cleaned_parquet"
    __fare_output_path = f"dataset/trip_fare_parquet"
    __fare_cleaned_output_path = f"dataset/trip_fare_cleaned_parquet"
    __merged_data_path = "dataset/trip_merged_date_parquet"

    def load_trip_data(self):
        if not exists(self.__trip_output_path):
            for i in range(1, 13):
                path = f"dataset/trip_data/trip_data_{i}.csv"
                if  not exists(path):
                    self.logger.error(f"Path '{path}' does not exist")

                df = self.spark.read.csv(
                    path, header=True, schema=self.trip_data_schema, inferSchema=False, sep=","
                )

                df.write.mode("append").parquet(self.__trip_output_path)
        else:
            self.logger.warning("initial parquet loading for trip data is completed")

        df = self.spark.read.parquet(self.__trip_output_path)
        self.logger.info(f"loaded {df.count()} initial trip data records")
        df.show(n=2)

        cleaned_df = self.clean_trip_data(df)

        cleaned_df.write.mode("overwrite").parquet(self.__trip_cleaned_output_path)

    def load_fare_data(self):
        if not exists(self.__fare_output_path):
            for i in range(1, 13):
                path = f"dataset/trip_fare/trip_fare_{i}.csv"
                if  not exists(path):
                    self.logger.error(f"Path '{path}' does not exist")

                df = self.spark.read.csv(
                    path, header=True, schema=self.fare_data_schema, inferSchema=False, sep=","
                )

                df.write.mode("append").parquet(self.__fare_output_path)
        else:
            self.logger.warning("initial parquet loading for trip data is completed")

        df = self.spark.read.parquet(self.__fare_output_path)
        self.logger.info(f"loaded {df.count()} initial fare data records")
        df.show(n=2)

        cleaned_df = self.clean_fare_data(df)

        cleaned_df.write.mode("overwrite").parquet(self.__fare_cleaned_output_path)


    def join_cleaned_data(self):
        trip_df = self.spark.read.schema(self.trip_data_schema).parquet(self.__trip_cleaned_output_path)
        fare_df = self.spark.read.schema(self.fare_data_schema).parquet(self.__fare_cleaned_output_path)

        trip_count = trip_df.count()
        fare_count = fare_df.count()
        print()
        self.logger.info(f"{trip_count}, {fare_count}")
        print()

        fare_df = broadcast(fare_df)

        for m in range(1, 32):
            self.logger.info(f"З’єднання за день: {m}")

            trip_chunk = trip_df.filter(dayofmonth("pickup_datetime") == m)
            print()
            print(trip_chunk.count())
            print()

            joined_chunk = trip_chunk.join(
                fare_df,
                on=["medallion", "hack_license", "vendor_id", "pickup_datetime"],
                how="inner"
            )

            joined_chunk.write.mode("append").parquet(self.__merged_data_path)

        joined_df = self.spark.read.parquet(self.__merged_data_path)

        joined_count = joined_df.count()
        
        self.logger.info(f"Записів про поїхдки {trip_count}, записів про комісію {fare_count}, з'єднані дані {joined_count} ({joined_count/trip_count * 100}, {joined_count/fare_count * 100})")

    def load_final_data(self):
        df = self.spark.read.parquet(self.__merged_data_path)
        df = self.clean_fare_data(df)
        df = self.clean_trip_data(df)
        return df
