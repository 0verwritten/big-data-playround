from ..base import BaseClass
from ..transformation.cleaning import Cleaning
class Loaders(BaseClass, Cleaning):
    def load_trip_data(self):
        for i in range(1, 13):
            path = f"dataset/trip_data/trip_data_{i}.csv"
            df = self.spark.read.csv(path, header=True, inferSchema=True)
            

            output_path = f"dataset/trip_data_parquet"
            df.write.mode("append").parquet(output_path)

        df = self.spark.read.parquet(output_path)
        self.clean_trip_data()
        

