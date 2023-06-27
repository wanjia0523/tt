from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("WineQualityPrediction").getOrCreate()

data = spark.read.csv("hdfs:///lab_test/wine.csv", header=True, inferSchema=True)

selected_columns = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar",
                    "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density",
                    "pH", "sulphates", "alcohol", "quality"]

data = data.select(selected_columns)

output_path = "hdfs:///lab_test/output"  # Specify the output directory path

data.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://127.0.0.1/wine_db.wine_collection") \
    .mode("append") \
    .save(output_path)  # Save the results to the specified output directory

spark.stop()
