from src.main_class import DataAnalysis

if __name__ == '__main__':
    c = DataAnalysis()

    # Initial Loading
    # c.load_trip_data()
    # c.load_fare_data()
    # c.join_cleaned_data()

    df = c.load_final_data()

    stats = c.get_basic_stats(df)

    for key, value in stats.items():
        print(key, value)

    c.write_results_to_csv(stats, "result/stats")

    results = c.process_all_business_questions(df)

    c.write_results_to_csv(results, "result/business")