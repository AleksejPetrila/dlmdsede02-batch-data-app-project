"""
This module takes the cleaned data from clean_data.py module and creates a very simple machine learning model.
The model creates a simple linear regression for 'Teens' age category, where the quantity of purchased products is
based solely on product's price. Root mean squared error, R2 and variance are used. as a measure of model accuracy.
After creating the model, the app places it in the data -> models directory. The app also creates a text file that shows
regression equation coefficient, intercept, r2, variance and root mean squared error.
If any model data or text file are present in the directory, they get overwritten.

The Spark session for this module is started and finished in the ml_model.py module.
"""
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from paths import HDFS_CLEANED_PATH, MODEL_PATH, COEFFICIENT_PATH


def linear_regression_price_total_age(spark):

    try:
        print(f"Loading clean data from {HDFS_CLEANED_PATH}.")
        df = spark.read.parquet(HDFS_CLEANED_PATH)
        print("Clean data loaded.")
    except Exception as e:
        print(f"Error with loading clean data from HDFS: {e}")
        spark.stop()
        return

    try:
        print("Filtering data for 'Teens' age group")
        age_group_filtered_df = df.filter(df["Customer Age Group"] == "Teens")
        # Check the filter by showing 5 rows of filtered df.
        age_group_filtered_df.show(5)
    except Exception as e:
        print(f"Error with filtering data by 'Teens' age group: {e}")
        spark.stop()
        return

    try:
        print("Selecting columns: 'Price' and 'Quantity Sold'.")
        age_group_filtered_df = age_group_filtered_df.select('Price', 'Quantity Sold')
        # Assemble the features into a single vector in order to make regression model (needed for Spark work).
        print("Assembling features into a single vector.")
        assembler = VectorAssembler(inputCols=['Price'], outputCol='features')
        data = assembler.transform(age_group_filtered_df)
        print("Features assembled successfully into a single vector.")
    except Exception as e:
        print(f"Error with assembling features into a single vector: {e}")
        spark.stop()
        return

    try:
        print("Creating linear regression model.")
        lr = LinearRegression(featuresCol='features', labelCol='Quantity Sold')
        lr_model = lr.fit(data)
        print("Model is created.")
    except Exception as e:
        print(f"Error with model creation: {e}")
        spark.stop()
        return

    try:
        # Checking the process by showing the coefficient and the intercept
        coefficients = lr_model.coefficients
        intercept = lr_model.intercept
        print(f"Coefficients: {coefficients}")
        print(f"Intercept: {intercept}")
        # Saving the model
        lr_model.write().overwrite().save(MODEL_PATH)
        print(f"The model is saved at: {MODEL_PATH}")
    except Exception as e:
        print(f"Error with saving the model: {e}")
        spark.stop()
        return

    try:
        # Make predictions of quantity sold based on the model and checking the process by showing five rows.
        print("Making predictions based on the created model")
        predictions = lr_model.transform(data)
        predictions.select('Price', 'Quantity Sold', 'prediction').show(5)

        # Evaluating the model with RMSE and showing it.
        print("Evaluating the model with Root Mean Squared Error (RMSE).")
        evaluator_rmse = RegressionEvaluator(labelCol='Quantity Sold', predictionCol='prediction', metricName='rmse')
        rmse = evaluator_rmse.evaluate(predictions)
        print(f"Root Mean Squared Error (RMSE): {rmse}")

        # Evaluating the model with variance and showing it.
        print("Evaluating the model with variance.")
        evaluator_var = RegressionEvaluator(labelCol='Quantity Sold', predictionCol='prediction', metricName='var')
        var = evaluator_var.evaluate(predictions)
        print(f"Variance: {var}")

        # Evaluating the model with r^2 (R squared) and showing it (determine correlation between variable).
        print("Evaluating the model with R2.")
        evaluator_r2 = RegressionEvaluator(labelCol='Quantity Sold', predictionCol='prediction', metricName='r2')
        r2 = evaluator_r2.evaluate(predictions)
        print(f"R2 Coefficient: {r2}")
    except Exception as e:
        print(f"Error with making predictions and evaluating the model: {e}")
        spark.stop()
        return

    try:
        # Create a text file and write the coefficient, the intercept and evaluators to a text file
        print(f"Creating a text file with coefficient, intercept and evaluators.")
        with open(COEFFICIENT_PATH, 'w') as f:
            f.write(f"Coefficient: {coefficients}\n")
            f.write(f"Intercept: {intercept}\n")
            f.write(f"Variance: {var}\n")
            f.write(f"R2: {r2}\n")
            f.write(f"RMSE: {rmse}\n")
        print(f"Text file is saved to: {COEFFICIENT_PATH}")
    except Exception as e:
        print(f"Error with creating a text file with a coefficient, intercept and RMSE: {e}")
        spark.stop()
        return
