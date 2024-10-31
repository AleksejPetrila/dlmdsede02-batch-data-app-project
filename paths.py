"""
This is the file where all used paths are present. In order to avoid hard coding the path values in actual modules,
please use this file as a single source of information about your path variables.

Make sure to create the directories first on your local machine and in HDFS before running the app, as missing
directories may lead to errors.

Make sure to give the spark app right permissions for HDFS directories, as missing permissions may lead to errors.
"""

# This is the path to the CSV file with initial data (used in ingest_data)
INGESTED_CSV_PATH = "/opt/spark-app/data/toy_store_data/toy_store_sales.csv"

# This is the path to HDFS storage for ingested data (used in ingest_data and clean_data)
HDFS_INGESTED_PATH = "hdfs://namenode:9000/user/hdfs/toy_store_sales_output"

# This is the path to HDFS storage for cleaned data (used in clean_data and regression_function)
HDFS_CLEANED_PATH = "hdfs://namenode:9000/user/hdfs/toy_store_clean_data"

# This is the path where the machine learning model is saved (used in regression_function)
MODEL_PATH = "/opt/spark-app/data/models/"

# This is the path where model coefficients, intercept, and RMSE will be saved as a text file (regression_function)
COEFFICIENT_PATH = "/opt/spark-app/data/models/model_coefficients.txt"
