"""
Data Processing Module

This module starts a Spark session and then calls ingest_data.py and clean_data.py modules for data ingestion and
cleaning. For more information, please read the comments in the respective module.

After data cleaning step is done, the spark session ends.

NB! Issues with safe mode.
It should be fixed now with adding polling function to start-hdfs.sh script, but if the error persists, try manually
run <docker exec -it namenode hdfs dfsadmin -safemode leave> command. Then, stop the multi-container application and try
to run it again. Sometimes running commands <docker-compose down> and then <docker-compose up -d> helps too.
"""

from pyspark.sql import SparkSession
from ingest_data import ingest_data
from clean_data import clean_data


def data_processing():
    try:
        print("Starting a spark session for data processing (ingestion and cleaning steps).")
        spark = SparkSession.builder \
            .appName("DataProcessingPipeline") \
            .getOrCreate()
        print("Spark session for data processing started successfully.")

        # Run ingest_data.py module
        print("Running ingest_data.py module")
        ingest_data(spark)

        # Run clean_data.py module
        print("Running clean_data.py module.")
        clean_data(spark)

    except Exception as e:
        print(f"Error with running data processing: {e}")

    finally:
        try:
            spark.stop()
            print("Spark session for data processing finished.")
        except Exception as e:
            print(f"Error with stopping spark session for data processing: {e}")


if __name__ == "__main__":
    data_processing()

