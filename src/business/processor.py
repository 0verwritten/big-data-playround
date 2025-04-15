from ..base import BaseClass
from . import Filters, Grouping, Joins, Windows

from pyspark.sql import DataFrame

class Processor(BaseClass, Filters, Grouping, Joins, Windows):
    def process_all_business_questions(self, df: DataFrame) -> dict:
        self.logger.info("Початок обробки бізнес-питань...")

        results = {}

        self.logger.info("Виконання запитів з фільтрацією...")
        results["get_longest_trips"] = self.get_longest_trips(df)
        results["get_most_expensive_trips"] = self.get_most_expensive_trips(df)
        results["get_night_trips"] = self.get_night_trips(df)
        results["get_weekend_trips"] = self.get_weekend_trips(df)
        results["get_rush_hour_trips"] = self.get_rush_hour_trips(df)
        results["get_short_trips"] = self.get_short_trips(df)
        results["get_high_tip_trips"] = self.get_high_tip_trips(df)
        results["get_high_speed_trips"] = self.get_high_speed_trips(df)
        results["get_many_passengers_trips"] = self.get_many_passengers_trips(df)

        self.logger.info("Виконання запитів з об'єднанням...")
        results["hourly_trip_fare"] = self.hourly_trip_fare(df)
        results["trips_by_day_of_week"] = self.trips_by_day_of_week(df)

        self.logger.info("Виконання запитів з групуванням...")
        results["payment_type_statistics"] = self.payment_type_statistics(df)
        results["passenger_count_fare_analysis"] = self.passenger_count_fare_analysis(df)
        results["day_of_week_tip_analysis"] = self.day_of_week_tip_analysis(df)
        results["highest_cost_per_mile_routes"] = self.highest_cost_per_mile_routes(df)
        results["area_trip_duration_analysis"] = self.area_trip_duration_analysis(df)
        results["distance_fare_correlation"] = self.distance_fare_correlation(df)
        results["trip_time_fare_analysis"] = self.trip_time_fare_analysis(df)
        results["trip_time_tip_correlation"] = self.trip_time_tip_correlation(df)

        self.logger.info("Виконання запитів з віконними функціями...")
        results["popular_routes_ranking"] = self.popular_routes_ranking(df)
        results["popular_pickup_areas_ranking"] = self.popular_pickup_areas_ranking(df)
        results["monthly_trips_comparison"] = self.monthly_trips_comparison(df)
        results["driver_speed_ranking"] = self.driver_speed_ranking(df)
        results["fare_comparison_by_day"] = self.fare_comparison_by_day(df)
        results["speed_comparison_by_day"] = self.speed_comparison_by_day(df)
        results["cumulative_monthly_trips"] = self.cumulative_monthly_trips(df)

        self.logger.info(f"Всього виконано {len(results)} бізнес-питань")
        return results