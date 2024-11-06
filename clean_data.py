"""
This module takes the data that was ingested into HDFS in ingest_data.py module and does data cleaning:
the records with missing values are removed, as well as the duplicates.
The cleaned data is then put into another HDFS directory for cleaned data.
If any data is present there it gets overwritten.

The Spark session for this module is started and finished in the data_processing.py module.
"""
from paths import HDFS_INGESTED_PATH, HDFS_CLEANED_PATH


def clean_data(spark):

    try:
        print(f"Reading ingested data from HDFS at {HDFS_INGESTED_PATH}...")
        df = spark.read.parquet(HDFS_INGESTED_PATH)
        print("Ingested data read successfully from HDFS.")
    except Exception as e:
        print(f"Error with reading ingested data from HDFS: {e}")
        spark.stop()
        return

    try:
        print("Removing records with missing values")
        df_cleaned = df.dropna()
        print("Missing values removed.")

        print("Removing duplicates")
        df_cleaned = df_cleaned.dropDuplicates()
        print("Duplicates removed.")
    except Exception as e:
        print(f"Error during data cleaning: {e}")
        spark.stop()
        return

    try:
        print(f"Writing cleaned data to HDFS at {HDFS_CLEANED_PATH}.")
        df_cleaned.write.mode("overwrite").parquet(HDFS_CLEANED_PATH)
        print(f"Cleaned data successfully written to HDFS at {HDFS_CLEANED_PATH}.")
    except Exception as e:
        print(f"Error with writing cleaned data to HDFS: {e}")
        spark.stop()
        return
