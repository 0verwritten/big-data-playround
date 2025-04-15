from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

class Schemas:
    @property
    def trip_data_schema(self):
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

    @property
    def fare_data_schema(self):
        """
        Схема для файлів fare_data.csv

        Містить інформацію про оплату:
        - medallion: ідентифікатор автомобіля
        - hack_license: ідентифікатор ліцензії водія
        - vendor_id: ідентифікатор постачальника
        - pickup_datetime: дата і час початку поїздки
        - payment_type: тип оплати
        - fare_amount: вартість поїздки
        - surcharge: додаткова плата
        - mta_tax: податок MTA
        - tip_amount: чайові
        - tolls_amount: плата за проїзд по платним дорогам
        - total_amount: загальна сума
        """
        return StructType([
            StructField("medallion", StringType(), False),
            StructField("hack_license", StringType(), False),
            StructField("vendor_id", StringType(), False),
            StructField("pickup_datetime", TimestampType(), False),
            StructField("payment_type", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("surcharge", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
    
    @property
    def merged_data_schema(self):
        pass