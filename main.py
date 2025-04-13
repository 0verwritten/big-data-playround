from src.main_class import DataAnalysis

if __name__ == '__main__':
    c = DataAnalysis()

    # c.load_trip_data()
    # c.load_fare_data()
    c.join_cleaned_data()