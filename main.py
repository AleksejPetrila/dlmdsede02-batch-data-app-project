"""
Main module that starts and runs the app. After the app is run the container stops in Docker. To run the app again,
start the Docker container again.

NB! Issues with safe mode.
It should be fixed now with adding polling function to start-hdfs.sh script, but if the error persists, try manually
run <docker exec -it namenode hdfs dfsadmin -safemode leave> command then run the spark
container again and everything works fine again.
"""
from ingest_data import ingest_data
from clean_data import clean_data
from regression_function import linear_regression_price_total_age


def main():
    # Runs the ingestion module
    try:
        ingest_data()
    except Exception as e:
        print(f"Error during ingestion: {e}")

    # Runs the data cleaning module
    try:
        clean_data()
    except Exception as e:
        print(f"Error during cleaning: {e}")

    # Runs the regression analysis module and finishes the work afterwards
    try:
        linear_regression_price_total_age()
    except Exception as e:
        print(f"Error during regression: {e}")


if __name__ == "__main__":
    main()
