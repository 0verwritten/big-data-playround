from pyspark.sql import DataFrame
from pyspark.sql.functions import count, countDistinct, min, max, avg, sum, stddev, col
from ..base import BaseClass

class Statistics(BaseClass):

    def get_basic_stats(self, joined_df: DataFrame) -> dict:
        self.logger.info("Обчислення базової статистики...")

        total_trips = joined_df.count()
        self.logger.info(f"Загальна кількість поїздок: {total_trips}")

        numeric_stats = joined_df.select(
            avg("passenger_count").alias("avg_passengers"),
            min("passenger_count").alias("min_passengers"),
            max("passenger_count").alias("max_passengers"),

            avg("trip_distance").alias("avg_distance"),
            min("trip_distance").alias("min_distance"),
            max("trip_distance").alias("max_distance"),
            stddev("trip_distance").alias("stddev_distance"),

            avg("trip_time_in_secs").alias("avg_duration"),
            min("trip_time_in_secs").alias("min_duration"),
            max("trip_time_in_secs").alias("max_duration"),

            avg("fare_amount").alias("avg_fare"),
            min("fare_amount").alias("min_fare"),
            max("fare_amount").alias("max_fare"),

            avg("total_amount").alias("avg_total"),
            min("total_amount").alias("min_total"),
            max("total_amount").alias("max_total"),

            avg("tip_amount").alias("avg_tip"),
            min("tip_amount").alias("min_tip"),
            max("tip_amount").alias("max_tip")
        ).collect()[0].asDict()

        unique_stats = {
            "unique_medallions": joined_df.select(countDistinct("medallion")).collect()[0][0],
            "unique_hack_licenses": joined_df.select(countDistinct("hack_license")).collect()[0][0],
            "unique_vendor_ids": joined_df.select(countDistinct("vendor_id")).collect()[0][0]
        }

        monthly_trips = joined_df.groupBy("pickup_month") \
            .count() \
            .orderBy("pickup_month") \
            .collect()

        monthly_trips_dict = {row["pickup_month"]: row["count"] for row in monthly_trips}

        hourly_trips = joined_df.groupBy("pickup_hour") \
            .count() \
            .orderBy("pickup_hour") \
            .collect()

        hourly_trips_dict = {row["pickup_hour"]: row["count"] for row in hourly_trips}

        dow_trips = joined_df.groupBy("pickup_dayofweek") \
            .count() \
            .orderBy("pickup_dayofweek") \
            .collect()

        dow_trips_dict = {row["pickup_dayofweek"]: row["count"] for row in dow_trips}

        payment_type_stats = joined_df.groupBy("payment_type") \
            .agg(
            count("*").alias("count"),
            avg("total_amount").alias("avg_amount")
        ) \
            .collect()

        payment_type_dict = {row["payment_type"]: {
            "count": row["count"],
            "avg_amount": row["avg_amount"]
        } for row in payment_type_stats}

        stats = {
            "total_trips": total_trips,
            "numeric_stats": numeric_stats,
            "unique_stats": unique_stats,
            "monthly_trips": monthly_trips_dict,
            "hourly_trips": hourly_trips_dict,
            "dow_trips": dow_trips_dict,
            "payment_type_stats": payment_type_dict
        }

        self.logger.info("Обчислення базової статистики завершено")
        return stats