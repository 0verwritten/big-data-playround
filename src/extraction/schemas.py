from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

class Schemas:
    @property
    def trip_data_schema():
        """
        Схема для файлів trip_data.csv

        Містить інформацію про поїздки:
        - medallion: ідентифікатор автомобіля
        - hack_license: ідентифікатор ліцензії водія
        - vendor_id: ідентифікатор постачальника
        - rate_code: код тарифу
        - store_and_fwd_flag: прапорець зберігання і пересилання
        - pickup_datetime: дата і час початку поїздки
        - dropoff_datetime: дата і час завершення поїздки
        - passenger_count: кількість пасажирів
        - trip_time_in_secs: тривалість поїздки у секундах
        - trip_distance: відстань поїздки у милях
        - pickup_longitude: довгота місця початку поїздки
        - pickup_latitude: широта місця початку поїздки
        - dropoff_longitude: довгота місця завершення поїздки
        - dropoff_latitude: широта місця завершення поїздки
        """
        return StructType([
            StructField("medallion", StringType(), False),
            StructField("hack_license", StringType(), False),
            StructField("vendor_id", StringType(), False),
            StructField("rate_code", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pickup_datetime", TimestampType(), False),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_time_in_secs", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True)
        ])
