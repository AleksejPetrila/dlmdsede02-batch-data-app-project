"""
Machine Learning Model Module

This module starts a Spark session and then calls regression_function.py module for ML model building and data analysis.
For more information, please read the comments in the respective module.

Currently, it is set a 60 seconds delay for the start of this module in docker-compose.yml file.

After data analysis and reporting, the spark session ends.

NB! Issues with safe mode.
It should be fixed now with adding polling function to start-hdfs.sh script, but if the error persists, try manually
run <docker exec -it namenode hdfs dfsadmin -safemode leave> command. Then, stop the multi-container application and try
to run it again. Sometimes running commands <docker-compose down> and then <docker-compose up -d> helps too.
"""

from pyspark.sql import SparkSession
from regression_function import linear_regression_price_total_age


def run_ml_model():
    try:
        # Start Spark session for ML model processing
        print("Starting spark session for machine learning model module")
        spark = SparkSession.builder \
            .appName("MLModelPipeline") \
            .getOrCreate()
        print("Spark session for machine learning model module started successfully.")

        # Run the regression_function.py module
        print("Running the regression_function.py module")
        linear_regression_price_total_age(spark)

    except Exception as e:
        print(f"Error in the regression_function.py module: {e}")

    finally:
        try:
            spark.stop()
            print("Spark session for machine learning model finished.")
        except Exception as e:
            print(f"Error during finishing the spark session for machine learning model: {e}")


if __name__ == "__main__":
    run_ml_model()
