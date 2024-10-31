"""
This module starts the Spark session, then takes the data from the data -> toy_store_data directory and ingests it
into the HDFS directory.
If there is already an ingested file there, the data will be overwritten.
After ingestion the spark session stops.
"""
from pyspark.sql import SparkSession
from paths import INGESTED_CSV_PATH, HDFS_INGESTED_PATH


def ingest_data():

    try:
        print("Starting Spark session for ingestion")
        spark = SparkSession.builder \
            .appName("CSVIngestion") \
            .getOrCreate()
        print("Spark session for ingestion started successfully.")
    except Exception as e:
        print(f"Error with starting Spark session for ingestion: {e}")
        return

    try:
        print(f"Ingesting the data from {INGESTED_CSV_PATH}.")
        df = spark.read.csv(INGESTED_CSV_PATH, header=True, inferSchema=True)
        print("CSV file read successfully.")
        # Show first five rows of the dataframe to verify ingestion.
        df.show(5)
    except Exception as e:
        print(f"Error with ingesting the data from CSV file: {e}")
        spark.stop()
        return

    try:
        print(f"Writing data to HDFS {HDFS_INGESTED_PATH} in Parquet format")
        df.write.mode("overwrite").parquet(HDFS_INGESTED_PATH)
        print(f"Data successfully written to HDFS {HDFS_INGESTED_PATH}.")
    except Exception as e:
        print(f"Error with writing data to HDFS: {e}")
        spark.stop()
        return

    try:
        spark.stop()
        print("Spark session for ingestion finished.")
    except Exception as e:
        print(f"Error during finish of Spark session for ingestion: {e}")